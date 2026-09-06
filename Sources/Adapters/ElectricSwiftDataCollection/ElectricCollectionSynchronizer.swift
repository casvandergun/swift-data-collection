import ElectricSwift
import Foundation
import SwiftData
import SwiftDataCollection

struct ElectricCollectionSynchronizer<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>: Sendable {
    let identifier: CollectionModelIdentifier<Model, ID>
    let rowDecoder: CollectionRowDecoder
    let collectionSchema: CollectionSchema
    let modelName: String
    let collectionID: String?
    let tracer: CollectionTracer
    let debugLogger: ElectricDebugLogger

    private enum UpsertChange {
        case inserted
        case updated
        case none
    }

    private struct UpsertOutcome {
        let resolvedTransactionIDs: Set<UUID>
        let change: UpsertChange
    }

    private struct DeleteOutcome {
        let resolvedTransactionIDs: Set<UUID>
        let deleted: Bool
    }

    private struct StagedEvent {
        let kind: CollectionTraceEventKind
        let key: String
        let operation: ElectricOperation
        let offset: String
        let outcome: String
    }

    init(
        identifier: CollectionModelIdentifier<Model, ID>,
        rowDecoder: CollectionRowDecoder = .init(),
        collectionSchema: CollectionSchema = .init(),
        modelName: String = String(reflecting: Model.self),
        collectionID: String? = nil,
        tracer: CollectionTracer = .disabled,
        debugLogger: ElectricDebugLogger = .disabled
    ) {
        self.identifier = identifier
        self.rowDecoder = rowDecoder
        self.collectionSchema = collectionSchema
        self.modelName = modelName
        self.collectionID = collectionID
        self.tracer = tracer
        self.debugLogger = debugLogger
    }

    func apply(
        _ batch: ShapeBatch,
        shapeID: String,
        in context: ModelContext
    ) throws -> ElectricShapeApplyResult {
        var resolvedTransactionIDs = Set<UUID>()
        let observedTXIDs = Set(batch.messages.flatMap { $0.headers.txids ?? [] })
        var insertedCount = 0
        var updatedCount = 0
        var deletedCount = 0
        var stagedEvents: [StagedEvent] = []
        let resolvedCollectionID = collectionID ?? "\(modelName):\(shapeID)"
        let materializer = CollectionMaterializer(
            context: context,
            collectionID: resolvedCollectionID,
            modelName: modelName,
            identifier: identifier,
            rowDecoder: rowDecoder
        )
        let metadata = try fetchMetadata(shapeID: shapeID, in: context)
            ?? ElectricShapeMetadata(shapeID: shapeID)
        if metadata.modelContext == nil {
            context.insert(metadata)
        }

        if batch.messages.contains(where: { $0.headers.control == .mustRefetch }) {
            try materializer.invalidateAllBaselines()
            metadata.beginAuthoritativeSnapshot()
            let refetch = try deleteRefetchableModels(in: context)
            deletedCount += refetch.deletedCount
            stagedEvents.append(contentsOf: refetch.preservedStagedKeys.map { key in
                StagedEvent(
                    kind: .stagedDeletePreserved,
                    key: key,
                    operation: .delete,
                    offset: batch.state.offset,
                    outcome: "preservedStagedReset"
                )
            })
            logApply(
                "cleared refetchable models",
                metadata: [
                    "shapeID": shapeID,
                    "modelName": modelName,
                    "collectionID": collectionID ?? "",
                    "deletedCount": String(refetch.deletedCount),
                    "preservedStagedKeys": refetch.preservedStagedKeys.joined(separator: ","),
                    "offset": batch.state.offset,
                ]
            )
        }

        for message in batch.messages {
            guard let operation = message.headers.operation else {
                logSkip(message, shapeID: shapeID, offset: batch.state.offset, reason: "no operation")
                continue
            }

            let key = message.normalizedKey ?? message.key
            let txids = message.headers.txids ?? []
            switch operation {
            case .insert, .update:
                guard let key else {
                    logSkip(message, shapeID: shapeID, offset: batch.state.offset, reason: "missing key or value")
                    continue
                }
                guard let row = message.value else {
                    logSkip(message, shapeID: shapeID, offset: batch.state.offset, reason: "missing key or value")
                    continue
                }
                let outcome = try applyUpsert(
                    operation: operation,
                    key: key,
                    row: row,
                    txids: txids,
                    message: message,
                    shapeID: shapeID,
                    batchState: batch.state,
                    stagedEvents: &stagedEvents,
                    materializer: materializer,
                    in: context
                )
                // Snapshot bookkeeping is limited to dirty keys. Clean rows do
                // not retain a base and must not turn this metadata into a
                // second copy of the shape.
                if try materializer.baselineEvidence(for: key) != nil {
                    try metadata.recordAuthoritativeSnapshotKey(key)
                }
                resolvedTransactionIDs.formUnion(outcome.resolvedTransactionIDs)
                switch outcome.change {
                case .inserted:
                    insertedCount += 1
                case .updated:
                    updatedCount += 1
                case .none:
                    break
                }
            case .delete:
                guard let key else {
                    logSkip(message, shapeID: shapeID, offset: batch.state.offset, reason: "missing key")
                    continue
                }
                let outcome = try applyDelete(
                    key: key,
                    txids: txids,
                    message: message,
                    shapeID: shapeID,
                    offset: batch.state.offset,
                    stagedEvents: &stagedEvents,
                    materializer: materializer,
                    in: context
                )
                if try materializer.baselineEvidence(for: key) != nil {
                    try metadata.recordAuthoritativeSnapshotKey(key)
                }
                resolvedTransactionIDs.formUnion(outcome.resolvedTransactionIDs)
                if outcome.deleted {
                    deletedCount += 1
                }
            }
        }

        if batch.boundaryKind == .upToDate, metadata.authoritativeSnapshotInProgress {
            let unseenKeys = try materializer.baselineKeys()
                .subtracting(metadata.authoritativeSnapshotSeenKeys())
            // Only bases invalidated by this snapshot are candidates for
            // inferred absence. A mutation captured after its row appeared in
            // the snapshot has observed evidence even though its key was not
            // in the reset's dirty-key set.
            let missingKeys = try unseenKeys.filter {
                try materializer.baselineEvidence(for: $0) == .unknown
            }
            for key in missingKeys {
                let existed = try fetchModel(key: key, in: context) != nil
                try materializer.apply(.absence, for: key)
                try materializer.materialize(key: key)
                if existed, try fetchModel(key: key, in: context) == nil {
                    deletedCount += 1
                }
            }
            metadata.finishAuthoritativeSnapshot()
        }
        metadata.apply(checkpoint: batch.checkpoint)
        logApply(
            "updated shape metadata",
            metadata: [
                "shapeID": shapeID,
                "modelName": modelName,
                "collectionID": collectionID ?? "",
                "offset": batch.checkpoint.offset,
                "phase": String(describing: batch.phase),
                "boundary": batch.boundaryKind.rawValue,
            ]
        )

        try context.save()
        for event in stagedEvents {
            traceStaged(
                event.kind,
                key: event.key,
                operation: event.operation,
                shapeID: shapeID,
                offset: event.offset,
                outcome: event.outcome
            )
        }
        let result = ElectricShapeApplyResult(
            resolvedTransactionIDs: Array(resolvedTransactionIDs),
            observedTXIDs: Array(observedTXIDs),
            insertedCount: insertedCount,
            updatedCount: updatedCount,
            deletedCount: deletedCount
        )
        logApply(
            "saved collection-aware SwiftData batch",
            metadata: [
                "shapeID": shapeID,
                "modelName": modelName,
                "collectionID": collectionID ?? "",
                "messages": String(batch.messages.count),
                "observedTXIDs": result.observedTXIDs.map(String.init).joined(separator: ","),
                "resolvedTransactions": String(result.resolvedTransactionIDs.count),
                "offset": batch.state.offset,
            ]
        )
        traceShapeBatchApplied(
            shapeID: shapeID,
            observedTXIDs: result.observedTXIDs,
            resolvedTransactionIDs: result.resolvedTransactionIDs,
            offset: batch.state.offset,
            metadata: [
                "messages": String(batch.messages.count),
                "insertedCount": String(insertedCount),
                "updatedCount": String(updatedCount),
                "deletedCount": String(deletedCount),
                "observedTXIDs": result.observedTXIDs.map(String.init).joined(separator: ","),
            ]
        )
        return result
    }

    private func applyUpsert(
        operation: ElectricOperation,
        key: String,
        row: ElectricRow,
        txids: [Int64],
        message: ElectricMessage,
        shapeID: String,
        batchState: ShapeStreamState,
        stagedEvents: inout [StagedEvent],
        materializer: CollectionMaterializer<Model, ID>,
        in context: ModelContext
    ) throws -> UpsertOutcome {
        let pending = unresolvedMutations(modelName: modelName, targetKey: key, in: context)
        let collectionRow = CollectionRow(
            electricRow: row,
            schema: batchState.schema,
            collectionSchema: collectionSchema
        )

        if pending.isEmpty {
            let stagedOutcome = try CollectionStagedReconciler.applyUpsert(
                key: key,
                row: collectionRow,
                mode: operation == .insert ? .replacement : .patch,
                identifier: identifier,
                rowDecoder: rowDecoder,
                in: context
            )
            if stagedOutcome == .resolved {
                logApply(
                    "resolved staged model from adapter upsert",
                    metadata: messageMetadata(
                        message,
                        shapeID: shapeID,
                        offset: batchState.offset,
                        extra: [
                            "modelName": modelName,
                            "collectionID": collectionID ?? "",
                            "key": key,
                            "outcome": "resolvedStaged",
                        ]
                    )
                )
                stagedEvents.append(
                    StagedEvent(
                        kind: .stagedInsertResolved,
                        key: key,
                        operation: operation,
                        offset: batchState.offset,
                        outcome: "resolved"
                    )
                )
                return UpsertOutcome(resolvedTransactionIDs: [], change: .updated)
            }
        }

        let localRowBefore = try fetchModel(key: key, in: context)?.collectionRow()
        let appliedEvidence = try materializer.apply(
            operation == .insert ? .replacement(collectionRow) : .patch(collectionRow),
            for: key
        )
        // Materialize while the optimistic overlay is still present. This
        // keeps the visible row coherent if acknowledgement resolution fails
        // later in the batch. A partial update over unknown/absent evidence is
        // intentionally not enough to acknowledge a token.
        try materializer.materialize(key: key)
        let resolvedTransactionIDs = appliedEvidence
            ? resolveAwaitingMutations(
                modelName: modelName,
                targetKey: key,
                txids: txids,
                allowedOperations: [.create, .update],
                in: context
            )
            : []
        if resolvedTransactionIDs.isEmpty == false {
            // Removing an acknowledged overlay changes the logical row. The
            // second materialization is still part of the caller's single
            // context save, so base, row, and outbox state commit together.
            try materializer.materialize(key: key)
        }
        let appliedModel = try fetchModel(key: key, in: context)
        let appliedRow = try appliedModel?.collectionRow()
        let protectedFields = Set(pending.filter { $0.operation != .delete }.flatMap(\.changedFields))
        let outcome = pending.isEmpty
            ? (operation == .update ? "mergedPatch" : (localRowBefore == nil ? "inserted" : "updated"))
            : (pending.contains { $0.operation == .delete } ? "pendingDelete" : "mergedPending")
        let logMessage = pending.isEmpty
            ? (operation == .update ? "merged patch into existing model" : (localRowBefore == nil ? "inserted new model" : "updated existing model"))
            : "merged server row into pending local model"
        logApply(
            logMessage,
            metadata: messageMetadata(
                message,
                shapeID: shapeID,
                offset: batchState.offset,
                extra: [
                    "modelName": modelName,
                    "collectionID": collectionID ?? "",
                    "key": key,
                    "pendingMutationCount": String(pending.count),
                    "protectedFields": protectedFields.sorted().joined(separator: ","),
                    "outcome": outcome,
                ]
            )
        )
        traceServerApply(
            message: logMessage,
            shapeID: shapeID,
            key: key,
            operation: operation,
            txids: txids,
            offset: batchState.offset,
            resolvedTransactionIDs: resolvedTransactionIDs,
            metadata: [
                "inboundRow": debugString(collectionRow),
                "localRowBefore": localRowBefore.map(debugString) ?? "nil",
                "appliedRow": appliedRow.map(debugString) ?? "nil",
                "changedFields": changedFields(for: collectionRow),
                "pendingMutationCount": String(pending.count),
                "protectedFields": protectedFields.sorted().joined(separator: ","),
                "outcome": outcome,
                "finalSyncState": appliedModel.map { String(describing: $0.collectionSyncState) } ?? "absent",
                "finalPendingMutationCount": appliedModel.map { String($0.collectionPendingMutationCount) } ?? "0",
            ]
        )
        let change: UpsertChange = if localRowBefore == nil && appliedRow != nil {
            .inserted
        } else if appliedRow != nil {
            .updated
        } else {
            .none
        }
        return UpsertOutcome(resolvedTransactionIDs: resolvedTransactionIDs, change: change)
    }

    private func applyDelete(
        key: String,
        txids: [Int64],
        message: ElectricMessage,
        shapeID: String,
        offset: String,
        stagedEvents: inout [StagedEvent],
        materializer: CollectionMaterializer<Model, ID>,
        in context: ModelContext
    ) throws -> DeleteOutcome {
        let pending = unresolvedMutations(modelName: modelName, targetKey: key, in: context)

        if pending.isEmpty,
           try CollectionStagedReconciler.preserveDelete(
               key: key,
               identifier: identifier,
               in: context
           ) == .preservedDelete {
            logApply(
                "preserved staged model during adapter delete",
                metadata: messageMetadata(
                    message,
                    shapeID: shapeID,
                    offset: offset,
                    extra: [
                        "modelName": modelName,
                        "collectionID": collectionID ?? "",
                        "key": key,
                        "outcome": "preservedStaged",
                    ]
                )
            )
            stagedEvents.append(
                StagedEvent(
                    kind: .stagedDeletePreserved,
                    key: key,
                    operation: .delete,
                    offset: offset,
                    outcome: "preservedStaged"
                )
            )
            return DeleteOutcome(resolvedTransactionIDs: [], deleted: false)
        }

        let existed = try fetchModel(key: key, in: context) != nil
        _ = try materializer.apply(.absence, for: key)
        try materializer.materialize(key: key)
        let resolvedTransactionIDs = resolveAwaitingMutations(
            modelName: modelName,
            targetKey: key,
            txids: txids,
            allowedOperations: [.delete],
            in: context
        )
        if resolvedTransactionIDs.isEmpty == false {
            try materializer.materialize(key: key)
        }
        let remains = try fetchModel(key: key, in: context) != nil
        let deleted = existed && !remains
        if deleted {
            logApply(
                "deleted model from SwiftData",
                metadata: messageMetadata(
                    message,
                    shapeID: shapeID,
                    offset: offset,
                    extra: [
                        "modelName": modelName,
                        "collectionID": collectionID ?? "",
                        "key": key,
                        "outcome": "deleted",
                    ]
                )
            )
            traceServerApply(
                message: "deleted model from SwiftData",
                shapeID: shapeID,
                key: key,
                operation: .delete,
                txids: txids,
                offset: offset,
                resolvedTransactionIDs: resolvedTransactionIDs,
                metadata: ["outcome": "deleted"]
            )
        } else {
            logApply(
                "delete skipped during reconciliation",
                metadata: messageMetadata(
                    message,
                    shapeID: shapeID,
                    offset: offset,
                    extra: [
                        "modelName": modelName,
                        "collectionID": collectionID ?? "",
                        "key": key,
                        "pendingMutationCount": String(pending.count),
                        "outcome": pending.isEmpty ? "missingModel" : "pendingState",
                    ]
                )
            )
            traceServerApply(
                message: "delete skipped during reconciliation",
                shapeID: shapeID,
                key: key,
                operation: .delete,
                txids: txids,
                offset: offset,
                resolvedTransactionIDs: resolvedTransactionIDs,
                metadata: [
                    "pendingMutationCount": String(pending.count),
                    "outcome": pending.isEmpty ? "missingModel" : "pendingState",
                ]
            )
        }
        return DeleteOutcome(resolvedTransactionIDs: resolvedTransactionIDs, deleted: deleted)
    }

    private func resolveAwaitingMutations(
        modelName: String,
        targetKey: String,
        txids: [Int64],
        allowedOperations: Set<CollectionMutationOperation>,
        in context: ModelContext
    ) -> Set<UUID> {
        guard txids.isEmpty == false else { return [] }

        var resolved = Set<UUID>()
        let observedTXIDs = Set(txids)
        let transactionsByID = pendingTransactionsByID(in: context)

        for mutation in unresolvedMutations(modelName: modelName, targetKey: targetKey, in: context)
        where allowedOperations.contains(mutation.operation)
            && mutation.status == .awaiting
            && transactionAwaitedTXIDs(
                for: mutation.transactionID,
                transactionsByID: transactionsByID
            ).isDisjoint(with: observedTXIDs) == false {
            mutation.status = .resolved
            mutation.errorMessage = nil
            resolved.insert(mutation.transactionID)
            if let collectionID {
                tracer.record(
                    CollectionTraceEvent(
                        kind: .mutationResolved,
                        collectionID: collectionID,
                        shapeID: mutation.shapeID,
                        modelName: modelName,
                        transactionID: mutation.transactionID,
                        key: targetKey,
                        operation: mutation.operation,
                        observedTokens: txids.map(String.init),
                        message: "resolved awaiting mutation from server batch"
                    )
                )
            }
        }
        return resolved
    }

    private func pendingTransactionsByID(in context: ModelContext) -> [UUID: PendingCollectionTransaction] {
        Dictionary(
            uniqueKeysWithValues: ((try? context.fetch(FetchDescriptor<PendingCollectionTransaction>())) ?? [])
                .map { ($0.id, $0) }
        )
    }

    private func transactionAwaitedTXIDs(
        for transactionID: UUID,
        transactionsByID: [UUID: PendingCollectionTransaction]
    ) -> Set<Int64> {
        Set(transactionsByID[transactionID]?.awaitedObservationTokens.compactMap(Int64.init) ?? [])
    }

    private func unresolvedMutations(
        modelName: String,
        targetKey: String,
        in context: ModelContext
    ) -> [PendingCollectionMutation] {
        ((try? context.fetch(FetchDescriptor<PendingCollectionMutation>())) ?? [])
            .filter { $0.modelName == modelName && $0.targetKey == targetKey }
            .filter { $0.status.participatesInReconciliationOverlay }
            .sorted { $0.createdAt < $1.createdAt }
    }

    private func fetchModel(key: String, in context: ModelContext) throws -> Model? {
        try context.fetch(identifier.fetchDescriptor(forSerializedKey: key)).first
    }

    private func fetchMetadata(shapeID: String, in context: ModelContext) throws -> ElectricShapeMetadata? {
        let descriptor = FetchDescriptor<ElectricShapeMetadata>(
            predicate: #Predicate<ElectricShapeMetadata> { $0.shapeID == shapeID }
        )
        return try context.fetch(descriptor).first
    }

    private func deleteRefetchableModels(
        in context: ModelContext
    ) throws -> (deletedCount: Int, preservedStagedKeys: [String]) {
        let models = try context.fetch(FetchDescriptor<Model>())
        var deletedCount = 0
        var preservedStagedKeys: [String] = []
        for model in models where model.collectionPendingMutationCount == 0 {
            if model.collectionSyncState == .stagedCreate {
                preservedStagedKeys.append(identifier.key(for: model))
                continue
            }
            context.delete(model)
            deletedCount += 1
        }
        return (deletedCount, preservedStagedKeys)
    }

    private func traceStaged(
        _ kind: CollectionTraceEventKind,
        key: String,
        operation: ElectricOperation,
        shapeID: String,
        offset: String,
        outcome: String
    ) {
        guard let collectionID else { return }
        tracer.record(
            CollectionTraceEvent(
                kind: kind,
                collectionID: collectionID,
                shapeID: shapeID,
                modelName: modelName,
                key: key,
                operation: collectionOperation(from: operation),
                offset: offset,
                message: kind == .stagedInsertResolved
                    ? "adapter resolved staged insert"
                    : "adapter preserved staged insert during delete",
                metadata: [
                    "outcome": outcome,
                    "adapterOperation": outcome == "preservedStagedReset"
                        ? "mustRefetch"
                        : String(describing: operation),
                    "previousSyncState": String(describing: CollectionSyncState.stagedCreate),
                    "resultingSyncState": kind == .stagedInsertResolved
                        ? String(describing: CollectionSyncState.synced)
                        : String(describing: CollectionSyncState.stagedCreate),
                ]
            )
        )
    }

    private func logSkip(
        _ message: ElectricMessage,
        shapeID: String,
        offset: String,
        reason: String
    ) {
        logApply(
            "skipped message during collection sync apply",
            metadata: messageMetadata(
                message,
                shapeID: shapeID,
                offset: offset,
                extra: [
                    "modelName": modelName,
                    "collectionID": collectionID ?? "",
                    "reason": reason,
                ]
            )
        )
    }

    private func logApply(
        _ message: String,
        metadata: [String: String]
    ) {
        debugLogger.log(
            .debug,
            category: "ShapeApply",
            message: message,
            metadata: metadata
        )
    }

    private func messageMetadata(
        _ message: ElectricMessage,
        shapeID: String,
        offset: String,
        extra: [String: String] = [:]
    ) -> [String: String] {
        var metadata: [String: String] = [
            "shapeID": shapeID,
            "offset": offset,
            "operation": message.headers.operation?.rawValue ?? "",
            "control": message.headers.control?.rawValue ?? "",
            "key": message.normalizedKey ?? message.key ?? "",
        ]
        if let txids = message.headers.txids, txids.isEmpty == false {
            metadata["txids"] = txids.map(String.init).joined(separator: ",")
        }
        for (key, value) in extra {
            metadata[key] = value
        }
        return metadata
    }

    private func traceServerApply(
        message: String,
        shapeID: String,
        key: String,
        operation: ElectricOperation,
        txids: [Int64],
        offset: String,
        resolvedTransactionIDs: Set<UUID>,
        metadata: [String: String]
    ) {
        guard let collectionID else { return }
        var values = metadata
        values["txids"] = txids.map(String.init).joined(separator: ",")
        values["observedTXIDs"] = txids.map(String.init).joined(separator: ",")
        values["offset"] = offset
        tracer.record(
            CollectionTraceEvent(
                kind: .shapeBatchApplied,
                collectionID: collectionID,
                shapeID: shapeID,
                modelName: modelName,
                key: key,
                operation: collectionOperation(from: operation),
                observedTokens: txids.map(String.init),
                resolvedTransactionIDs: Array(resolvedTransactionIDs).sorted { $0.uuidString < $1.uuidString },
                offset: offset,
                message: message,
                metadata: values
            )
        )
    }

    private func traceShapeBatchApplied(
        shapeID: String,
        observedTXIDs: [Int64],
        resolvedTransactionIDs: [UUID],
        offset: String,
        metadata: [String: String]
    ) {
        guard let collectionID else { return }
        tracer.record(
            CollectionTraceEvent(
                kind: .adapterBatchObserved,
                collectionID: collectionID,
                shapeID: shapeID,
                modelName: modelName,
                observedTokens: observedTXIDs.map(String.init),
                resolvedTransactionIDs: resolvedTransactionIDs.sorted { $0.uuidString < $1.uuidString },
                offset: offset,
                message: "applied collection-aware server batch",
                metadata: metadata
            )
        )
    }

    private func collectionOperation(from operation: ElectricOperation) -> CollectionMutationOperation? {
        switch operation {
        case .insert: .create
        case .update: .update
        case .delete: .delete
        }
    }

    private func debugString<T: Encodable>(_ value: T) -> String {
        guard let data = try? JSONEncoder().encode(value),
              let string = String(data: data, encoding: .utf8) else {
            return String(describing: value)
        }
        return string
    }

    private func changedFields(for row: CollectionRow) -> String {
        row.keys
            .filter { Self.syncMetadataFieldNames.contains($0) == false }
            .sorted()
            .joined(separator: ",")
    }

    private static var syncMetadataFieldNames: Set<String> {
        [
            "collectionPendingMutationCount",
            "collectionSyncState",
        ]
    }
}
