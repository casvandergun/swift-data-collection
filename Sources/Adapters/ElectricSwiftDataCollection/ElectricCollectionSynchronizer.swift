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

        if batch.messages.contains(where: { $0.headers.control == .mustRefetch }) {
            let refetchDeletedCount = try deleteRefetchableModels(in: context)
            deletedCount += refetchDeletedCount
            logApply(
                "cleared refetchable models",
                metadata: [
                    "shapeID": shapeID,
                    "modelName": modelName,
                    "collectionID": collectionID ?? "",
                    "deletedCount": String(refetchDeletedCount),
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
                guard let key, let row = message.value else {
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
                    in: context
                )
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
                    in: context
                )
                resolvedTransactionIDs.formUnion(outcome.resolvedTransactionIDs)
                if outcome.deleted {
                    deletedCount += 1
                }
            }
        }

        let metadata = try fetchMetadata(shapeID: shapeID, in: context)
            ?? ElectricShapeMetadata(shapeID: shapeID)
        if metadata.modelContext == nil {
            context.insert(metadata)
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
        in context: ModelContext
    ) throws -> UpsertOutcome {
        let resolvedTransactionIDs = resolveAwaitingMutations(
            modelName: modelName,
            targetKey: key,
            txids: txids,
            in: context
        )
        let pending = unresolvedMutations(modelName: modelName, targetKey: key, in: context)

        if pending.contains(where: { $0.operation == .delete }) {
            logApply(
                "skipped upsert because local delete is pending",
                metadata: messageMetadata(
                    message,
                    shapeID: shapeID,
                    offset: batchState.offset,
                    extra: [
                        "modelName": modelName,
                        "collectionID": collectionID ?? "",
                        "key": key,
                        "pendingMutationCount": String(pending.count),
                        "outcome": "pendingDelete",
                    ]
                )
            )
            return UpsertOutcome(resolvedTransactionIDs: resolvedTransactionIDs, change: .none)
        }

        let protectedFields = Set(
            pending
                .filter { $0.operation != .delete }
                .flatMap(\.changedFields)
        )

        if let existing = try fetchModel(key: key, in: context) {
            let localRow = try existing.collectionRow()
            let collectionRow = CollectionRow(
                electricRow: row,
                schema: batchState.schema,
                collectionSchema: collectionSchema
            )
            if pending.isEmpty {
                let appliedRow = if operation == .update {
                    CollectionRowPatcher.applying(patch: collectionRow, to: localRow)
                } else {
                    collectionRow
                }
                try existing.apply(collectionRow: appliedRow, decoder: rowDecoder)
                existing.collectionPendingMutationCount = 0
                existing.collectionSyncState = .synced
                logApply(
                    operation == .update ? "merged patch into existing model" : "updated existing model",
                    metadata: messageMetadata(
                        message,
                        shapeID: shapeID,
                        offset: batchState.offset,
                        extra: [
                            "modelName": modelName,
                            "collectionID": collectionID ?? "",
                            "key": key,
                            "outcome": operation == .update ? "mergedPatch" : "updated",
                        ]
                    )
                )
                traceServerApply(
                    message: operation == .update ? "merged patch into existing model" : "updated existing model",
                    shapeID: shapeID,
                    key: key,
                    operation: operation,
                    txids: txids,
                    offset: batchState.offset,
                    resolvedTransactionIDs: resolvedTransactionIDs,
                    metadata: [
                        "inboundRow": debugString(collectionRow),
                        "localRowBefore": debugString(localRow),
                        "appliedRow": debugString(appliedRow),
                        "changedFields": changedFields(for: collectionRow),
                        "outcome": operation == .update ? "mergedPatch" : "updated",
                        "finalSyncState": String(describing: existing.collectionSyncState),
                        "finalPendingMutationCount": String(existing.collectionPendingMutationCount),
                    ]
                )
            } else {
                let mergedRow = CollectionRowPatcher.applying(
                    patch: collectionRow,
                    to: localRow,
                    preserving: protectedFields
                )
                try existing.apply(collectionRow: mergedRow, decoder: rowDecoder)
                applyPendingSummary(pending, to: existing)
                logApply(
                    "merged server row into pending local model",
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
                            "outcome": "mergedPending",
                        ]
                    )
                )
                traceServerApply(
                    message: "merged server row into pending local model",
                    shapeID: shapeID,
                    key: key,
                    operation: operation,
                    txids: txids,
                    offset: batchState.offset,
                    resolvedTransactionIDs: resolvedTransactionIDs,
                    metadata: [
                        "inboundRow": debugString(collectionRow),
                        "localRowBefore": debugString(localRow),
                        "appliedRow": debugString(mergedRow),
                        "changedFields": changedFields(for: collectionRow),
                        "pendingMutationCount": String(pending.count),
                        "protectedFields": protectedFields.sorted().joined(separator: ","),
                        "outcome": "mergedPending",
                        "finalSyncState": String(describing: existing.collectionSyncState),
                        "finalPendingMutationCount": String(existing.collectionPendingMutationCount),
                    ]
                )
            }
            return UpsertOutcome(resolvedTransactionIDs: resolvedTransactionIDs, change: .updated)
        }

        let collectionRow = CollectionRow(
            electricRow: row,
            schema: batchState.schema,
            collectionSchema: collectionSchema
        )
        let merged = pending.isEmpty
            ? collectionRow
            : CollectionRowPatcher.applying(
                patch: collectionRow,
                to: [:],
                preserving: protectedFields
            )
        let model = try Model(collectionRow: merged, decoder: rowDecoder)
        if pending.isEmpty {
            model.collectionPendingMutationCount = 0
            model.collectionSyncState = .synced
            logApply(
                "inserted new model",
                metadata: messageMetadata(
                    message,
                    shapeID: shapeID,
                    offset: batchState.offset,
                    extra: [
                        "modelName": modelName,
                        "collectionID": collectionID ?? "",
                        "key": key,
                        "outcome": "inserted",
                    ]
                )
            )
            traceServerApply(
                message: "inserted new model",
                shapeID: shapeID,
                key: key,
                operation: operation,
                txids: txids,
                offset: batchState.offset,
                resolvedTransactionIDs: resolvedTransactionIDs,
                metadata: [
                    "inboundRow": debugString(collectionRow),
                    "appliedRow": debugString(merged),
                    "changedFields": changedFields(for: collectionRow),
                    "outcome": "inserted",
                    "finalSyncState": String(describing: model.collectionSyncState),
                    "finalPendingMutationCount": String(model.collectionPendingMutationCount),
                ]
            )
        } else {
            applyPendingSummary(pending, to: model)
            logApply(
                "inserted model while preserving pending local fields",
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
                        "outcome": "insertedPending",
                    ]
                )
            )
            traceServerApply(
                message: "inserted model while preserving pending local fields",
                shapeID: shapeID,
                key: key,
                operation: operation,
                txids: txids,
                offset: batchState.offset,
                resolvedTransactionIDs: resolvedTransactionIDs,
                metadata: [
                    "inboundRow": debugString(collectionRow),
                    "appliedRow": debugString(merged),
                    "changedFields": changedFields(for: collectionRow),
                    "pendingMutationCount": String(pending.count),
                    "protectedFields": protectedFields.sorted().joined(separator: ","),
                    "outcome": "insertedPending",
                    "finalSyncState": String(describing: model.collectionSyncState),
                    "finalPendingMutationCount": String(model.collectionPendingMutationCount),
                ]
            )
        }
        context.insert(model)
        return UpsertOutcome(resolvedTransactionIDs: resolvedTransactionIDs, change: .inserted)
    }

    private func applyDelete(
        key: String,
        txids: [Int64],
        message: ElectricMessage,
        shapeID: String,
        offset: String,
        in context: ModelContext
    ) throws -> DeleteOutcome {
        let resolvedTransactionIDs = resolveAwaitingMutations(
            modelName: modelName,
            targetKey: key,
            txids: txids,
            in: context
        )
        let pending = unresolvedMutations(modelName: modelName, targetKey: key, in: context)

        if pending.contains(where: { $0.operation == .delete }) {
            logApply(
                "skipped delete because local delete is pending",
                metadata: messageMetadata(
                    message,
                    shapeID: shapeID,
                    offset: offset,
                    extra: [
                        "modelName": modelName,
                        "collectionID": collectionID ?? "",
                        "key": key,
                        "pendingMutationCount": String(pending.count),
                        "outcome": "pendingDelete",
                    ]
                )
            )
            return DeleteOutcome(resolvedTransactionIDs: resolvedTransactionIDs, deleted: false)
        }

        var deleted = false
        if pending.isEmpty, let existing = try fetchModel(key: key, in: context) {
            context.delete(existing)
            deleted = true
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
        in context: ModelContext
    ) -> Set<UUID> {
        guard txids.isEmpty == false else { return [] }

        var resolved = Set<UUID>()
        let observedTXIDs = Set(txids)
        let transactionsByID = pendingTransactionsByID(in: context)

        for mutation in unresolvedMutations(modelName: modelName, targetKey: targetKey, in: context)
        where mutation.status == .awaitingSync
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

    private func applyPendingSummary(_ pending: [PendingCollectionMutation], to model: Model) {
        model.collectionPendingMutationCount = pending.count
        if pending.contains(where: { $0.status == .failed }) {
            model.collectionSyncState = .syncError
        } else if pending.contains(where: { $0.operation == .create }) {
            model.collectionSyncState = .pendingCreate
        } else if pending.contains(where: { $0.operation == .delete }) {
            model.collectionSyncState = .pendingDelete
        } else {
            model.collectionSyncState = .pendingUpdate
        }
    }

    private func unresolvedMutations(
        modelName: String,
        targetKey: String,
        in context: ModelContext
    ) -> [PendingCollectionMutation] {
        ((try? context.fetch(FetchDescriptor<PendingCollectionMutation>())) ?? [])
            .filter { $0.modelName == modelName && $0.targetKey == targetKey }
            .filter { mutation in
                switch mutation.status {
                case .pending, .sending, .awaitingSync, .failed:
                    return true
                case .resolved, .conflicted:
                    return false
                }
            }
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

    private func deleteRefetchableModels(in context: ModelContext) throws -> Int {
        let models = try context.fetch(FetchDescriptor<Model>())
        var deletedCount = 0
        for model in models where model.collectionPendingMutationCount == 0 {
            context.delete(model)
            deletedCount += 1
        }
        return deletedCount
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
