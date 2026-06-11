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
    private let identifier: CollectionModelIdentifier<Model, ID>
    private let rowDecoder: CollectionRowDecoder
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
            modelName: configuration.modelName
        )
        self.identifier = context.identifier
        self.rowDecoder = context.rowDecoder
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
            try applier.apply(rows, in: modelContext)
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

    func apply(
        _ rows: [CollectionRow],
        in context: ModelContext
    ) throws {
        let existingModels = try context.fetch(FetchDescriptor<Model>())
        let unresolvedMutations = unresolvedMutationsByKey(in: context)
        var returnedKeys = Set<String>()

        for row in rows {
            let decoded = try Model(collectionRow: row, decoder: rowDecoder)
            let key = identifier.key(for: decoded)
            guard key.isEmpty == false else {
                throw FetchCollectionError.missingStableIdentifier
            }

            returnedKeys.insert(key)
            if let existing = try fetchModel(key: key, in: context) {
                try existing.apply(collectionRow: row, decoder: rowDecoder)
                refreshSyncState(
                    for: existing,
                    pending: unresolvedMutations[key] ?? []
                )
            } else {
                decoded.collectionSyncState = .synced
                decoded.collectionPendingMutationCount = 0
                refreshSyncState(
                    for: decoded,
                    pending: unresolvedMutations[key] ?? []
                )
                context.insert(decoded)
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
    }

    private func fetchModel(key: String, in context: ModelContext) throws -> Model? {
        try context.fetch(identifier.fetchDescriptor(forSerializedKey: key)).first
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

