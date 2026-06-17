import Foundation
import SwiftData
import SwiftDataCollection

actor FetchCollectionAdapterRuntime<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: CollectionAdapterRuntime {
    private let configuration: FetchCollectionOptions<Model, ID>
    private let modelContainer: ModelContainer
    private let context: FetchCollectionContext<Model, ID>
    private let collectionID: String
    private let sourceID: String
    private let identifier: CollectionModelIdentifier<Model, ID>
    private let rowDecoder: CollectionRowDecoder
    private let onApply: CollectionApplyHandler?
    private let reportRefreshCompleted: @Sendable (Date?) async -> Void
    private let reportError: @Sendable (Error) async -> Void

    init(
        configuration: FetchCollectionOptions<Model, ID>,
        context: CollectionAdapterContext<Model, ID>
    ) {
        self.configuration = configuration
        self.modelContainer = context.modelContainer
        self.context = FetchCollectionContext(
            debugName: context.debugName,
            modelName: configuration.modelName,
            scopeID: configuration.scopeID
        )
        self.collectionID = context.collectionID
        self.sourceID = context.sourceID
        self.identifier = context.identifier
        self.rowDecoder = context.rowDecoder
        self.onApply = context.onApply
        self.reportRefreshCompleted = context.reportRefreshCompleted
        self.reportError = context.reportError
    }

    func start() async {
        await refresh()
    }

    func stop() async {}

    func refresh() async {
        do {
            let rows = try await configuration.fetch(context)
            let modelContext = ModelContext(modelContainer)
            let applier = FetchCollectionSnapshotApplier(
                identifier: identifier,
                rowDecoder: rowDecoder,
                modelName: configuration.modelName,
                missingRowPolicy: configuration.missingRowPolicy
            )
            let result = try applier.apply(rows, in: modelContext)
            let summary = CollectionBatchApplySummary(
                collectionIdentifier: collectionID,
                sourceIdentifier: sourceID,
                insertedCount: result.insertedCount,
                updatedCount: result.updatedCount,
                deletedCount: result.deletedCount
            )
            try onApply?(modelContext, summary)
            await reportRefreshCompleted(Date())
        } catch {
            await reportError(error)
        }
    }
}

struct FetchCollectionSnapshotApplier<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> {
    let identifier: CollectionModelIdentifier<Model, ID>
    let rowDecoder: CollectionRowDecoder
    let modelName: String
    let missingRowPolicy: FetchMissingRowPolicy

    struct ApplyResult: Sendable, Hashable {
        let insertedCount: Int
        let updatedCount: Int
        let deletedCount: Int
    }

    func apply(
        _ rows: [CollectionRow],
        in context: ModelContext
    ) throws -> ApplyResult {
        let existingModels = try context.fetch(FetchDescriptor<Model>())
        let unresolvedMutations = unresolvedMutationsByKey(in: context)
        var returnedKeys = Set<String>()
        var insertedCount = 0
        var updatedCount = 0
        var deletedCount = 0

        for row in rows {
            let decoded = try Model(collectionRow: row, decoder: rowDecoder)
            let key = identifier.key(for: decoded)
            guard key.isEmpty == false else {
                throw FetchCollectionError.missingStableIdentifier
            }

            returnedKeys.insert(key)
            if let existing = try fetchModel(key: key, in: context) {
                if try applyFetchedRow(
                    row,
                    to: existing,
                    pending: unresolvedMutations[key] ?? []
                ) {
                    updatedCount += 1
                }
            } else {
                let pending = unresolvedMutations[key] ?? []
                guard pending.contains(where: { $0.operation == .delete }) == false else {
                    continue
                }
                try applyPendingMutationPayloads(to: decoded, pending: pending)
                refreshSyncState(for: decoded, pending: pending)
                context.insert(decoded)
                insertedCount += 1
            }
        }

        switch missingRowPolicy {
        case .deleteSyncedRows:
            for model in existingModels {
                let key = identifier.key(for: model)
                guard returnedKeys.contains(key) == false else { continue }
                let pending = unresolvedMutations[key] ?? []
                if pending.isEmpty {
                    context.delete(model)
                    deletedCount += 1
                } else {
                    refreshSyncState(for: model, pending: pending)
                }
            }
        case .keepLocalRows:
            for model in existingModels {
                let key = identifier.key(for: model)
                guard returnedKeys.contains(key) == false else { continue }
                refreshSyncState(
                    for: model,
                    pending: unresolvedMutations[key] ?? []
                )
            }
        }

        try context.save()
        return ApplyResult(
            insertedCount: insertedCount,
            updatedCount: updatedCount,
            deletedCount: deletedCount
        )
    }

    private func fetchModel(key: String, in context: ModelContext) throws -> Model? {
        try context.fetch(identifier.fetchDescriptor(forSerializedKey: key)).first
    }

    private func applyFetchedRow(
        _ row: CollectionRow,
        to model: Model,
        pending: [PendingCollectionMutation]
    ) throws -> Bool {
        guard pending.isEmpty == false else {
            try model.apply(collectionRow: row, decoder: rowDecoder)
            model.collectionPendingMutationCount = 0
            model.collectionSyncState = .synced
            return true
        }

        if pending.contains(where: { $0.status == .failed }) ||
            pending.contains(where: { $0.operation == .delete }) {
            refreshSyncState(for: model, pending: pending)
            return false
        }

        let protectedFields = protectedPendingFields(pending)
        let merged = CollectionRowPatcher.applying(
            patch: row,
            to: try model.collectionRow(),
            preserving: protectedFields
        )
        try model.apply(collectionRow: merged, decoder: rowDecoder)
        refreshSyncState(for: model, pending: pending)
        return true
    }

    private func applyPendingMutationPayloads(
        to model: Model,
        pending: [PendingCollectionMutation]
    ) throws {
        guard pending.isEmpty == false else { return }
        guard pending.contains(where: { $0.status == .failed }) == false else { return }

        var row = try model.collectionRow()
        for mutation in pending where mutation.operation != .delete {
            row = CollectionRowPatcher.applying(
                patch: mutation.payload,
                to: row
            )
        }
        try model.apply(collectionRow: row, decoder: rowDecoder)
    }

    private func protectedPendingFields(_ pending: [PendingCollectionMutation]) -> Set<String> {
        Set(
            pending
                .filter { $0.operation != .delete }
                .flatMap(\.changedFields)
        )
    }

    private func unresolvedMutationsByKey(
        in context: ModelContext
    ) -> [String: [PendingCollectionMutation]] {
        let mutations = ((try? context.fetch(FetchDescriptor<PendingCollectionMutation>())) ?? [])
            .filter { $0.modelName == modelName }
            .filter { mutation in
                switch mutation.status {
                case .pending, .sending, .awaitingSync, .failed:
                    return true
                case .resolved, .conflicted:
                    return false
                }
            }

        return Dictionary(grouping: mutations, by: \.targetKey)
    }

    private func refreshSyncState(
        for model: Model,
        pending: [PendingCollectionMutation]
    ) {
        model.collectionPendingMutationCount = pending.count
        if pending.isEmpty {
            model.collectionSyncState = .synced
        } else if pending.contains(where: { $0.status == .failed }) {
            model.collectionSyncState = .syncError
        } else if pending.contains(where: { $0.operation == .delete }) {
            model.collectionSyncState = .pendingDelete
        } else if pending.contains(where: { $0.operation == .create }) {
            model.collectionSyncState = .pendingCreate
        } else {
            model.collectionSyncState = .pendingUpdate
        }
    }
}

public enum FetchCollectionError: Error, Sendable {
    case missingStableIdentifier
}
