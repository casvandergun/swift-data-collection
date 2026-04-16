import ElectricSwift
import Foundation
import SwiftData
import SwiftDataCollection

public struct ElectricShapeApplyResult: Sendable, Hashable {
    public let resolvedTransactionIDs: [UUID]
    public let observedTXIDs: [Int64]

    public init(resolvedTransactionIDs: [UUID] = [], observedTXIDs: [Int64] = []) {
        self.resolvedTransactionIDs = resolvedTransactionIDs
        self.observedTXIDs = observedTXIDs
    }
}

public typealias ElectricShapeBatchApplyClosure =
    @Sendable (ShapeBatch, String, ModelContext) throws -> ElectricShapeApplyResult

public struct ElectricSwiftDataRowApplier<Model: SwiftDataCollectionModel, ID: Hashable & Sendable> {
    public let rowDecoder: CollectionRowDecoder
    public let identifier: CollectionModelIdentifier<Model, ID>
    public let debugLogger: ElectricDebugLogger

    private enum UpsertOutcome: String {
        case inserted
        case updated
        case mergedPatch
    }

    public init(
        identifier: CollectionModelIdentifier<Model, ID>,
        rowDecoder: CollectionRowDecoder = .init(),
        debugLogger: ElectricDebugLogger = .disabled
    ) {
        self.identifier = identifier
        self.rowDecoder = rowDecoder
        self.debugLogger = debugLogger
    }

    public func apply(
        _ batch: ShapeBatch,
        shapeID: String,
        in context: ModelContext
    ) throws -> ElectricShapeApplyResult {
        if batch.messages.contains(where: { $0.headers.control == .mustRefetch }) {
            let deletedCount = try deleteRefetchableModels(in: context)
            logApply(
                shapeID: shapeID,
                message: "cleared refetchable models",
                metadata: [
                    "modelName": modelName,
                    "deletedCount": String(deletedCount),
                    "offset": batch.state.offset,
                ]
            )
        }

        for message in batch.messages {
            guard let operation = message.headers.operation else {
                logMessageSkip(message, shapeID: shapeID, offset: batch.state.offset, reason: "no operation")
                continue
            }

            let key = message.normalizedKey ?? message.key
            switch operation {
            case .insert, .update:
                guard let key, let row = message.value else {
                    logMessageSkip(message, shapeID: shapeID, offset: batch.state.offset, reason: "missing key or value")
                    continue
                }
                let outcome = try applyUpsert(
                    operation: operation,
                    key: key,
                    row: row,
                    in: context
                )
                logApply(
                    shapeID: shapeID,
                    message: outcome == .mergedPatch ? "merged patch into existing model" : "applied row mutation",
                    metadata: messageMetadata(
                        message,
                        shapeID: shapeID,
                        offset: batch.state.offset,
                        extra: [
                            "modelName": modelName,
                            "key": key,
                            "outcome": outcome.rawValue,
                        ]
                    )
                )
            case .delete:
                guard let key else {
                    logMessageSkip(message, shapeID: shapeID, offset: batch.state.offset, reason: "missing key")
                    continue
                }
                let deleted = try applyDelete(key: key, in: context)
                logApply(
                    shapeID: shapeID,
                    message: deleted ? "deleted model from SwiftData" : "delete skipped; model not found",
                    metadata: messageMetadata(
                        message,
                        shapeID: shapeID,
                        offset: batch.state.offset,
                        extra: [
                            "modelName": modelName,
                            "key": key,
                            "outcome": deleted ? "deleted" : "missingModel",
                        ]
                    )
                )
            }
        }

        let metadata = try fetchMetadata(shapeID: shapeID, in: context)
            ?? ElectricShapeMetadata(shapeID: shapeID)
        if metadata.modelContext == nil {
            context.insert(metadata)
        }
        metadata.apply(checkpoint: batch.checkpoint)
        logApply(
            shapeID: shapeID,
            message: "updated shape metadata",
            metadata: [
                "modelName": modelName,
                "shapeID": shapeID,
                "offset": batch.checkpoint.offset,
                "phase": String(describing: batch.phase),
                "boundary": batch.boundaryKind.rawValue,
            ]
        )

        try context.save()
        let result = ElectricShapeApplyResult(
            resolvedTransactionIDs: [],
            observedTXIDs: Array(Set(batch.messages.flatMap { $0.headers.txids ?? [] }))
        )
        logApply(
            shapeID: shapeID,
            message: "saved SwiftData batch",
            metadata: [
                "modelName": modelName,
                "shapeID": shapeID,
                "messages": String(batch.messages.count),
                "observedTXIDs": result.observedTXIDs.map(String.init).joined(separator: ","),
                "offset": batch.state.offset,
            ]
        )
        return result
    }

    private func applyUpsert(
        operation: ElectricOperation,
        key: String,
        row: ElectricRow,
        in context: ModelContext
    ) throws -> UpsertOutcome {
        let collectionRow = CollectionRow(electricRow: row)
        if let existing = try fetchModel(key: key, in: context) {
            let appliedRow = if operation == .update {
                CollectionRowPatcher.applying(patch: collectionRow, to: try existing.collectionRow())
            } else {
                collectionRow
            }
            try existing.apply(collectionRow: appliedRow, decoder: rowDecoder)
            existing.collectionPendingMutationCount = 0
            existing.collectionSyncState = .synced
            return operation == .update ? .mergedPatch : .updated
        }

        let model = try Model(collectionRow: collectionRow, decoder: rowDecoder)
        model.collectionPendingMutationCount = 0
        model.collectionSyncState = .synced
        context.insert(model)
        return .inserted
    }

    private func applyDelete(
        key: String,
        in context: ModelContext
    ) throws -> Bool {
        if let existing = try fetchModel(key: key, in: context) {
            context.delete(existing)
            return true
        }
        return false
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

    private var modelName: String {
        String(reflecting: Model.self)
    }

    private func logMessageSkip(
        _ message: ElectricMessage,
        shapeID: String,
        offset: String,
        reason: String
    ) {
        logApply(
            shapeID: shapeID,
            message: "skipped message during SwiftData apply",
            metadata: messageMetadata(
                message,
                shapeID: shapeID,
                offset: offset,
                extra: [
                    "modelName": modelName,
                    "reason": reason,
                ]
            )
        )
    }

    private func logApply(
        shapeID: String,
        message: String,
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
}
