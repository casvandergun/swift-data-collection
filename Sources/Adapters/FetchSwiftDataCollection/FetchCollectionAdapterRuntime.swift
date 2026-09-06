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
    private let writeGate: CollectionWriteGate
    private let tracer: CollectionTracer
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
        self.writeGate = context.writeGate
        self.tracer = context.tracer
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
            try writeGate.withCriticalSection {
                let modelContext = ModelContext(modelContainer)
                let applier = FetchCollectionSnapshotApplier(
                    identifier: identifier,
                    rowDecoder: rowDecoder,
                    collectionID: collectionID,
                    modelName: configuration.modelName,
                    missingRowPolicy: configuration.missingRowPolicy
                )
                // A successful fetch is the Fetch adapter's authoritative
                // completion boundary. The applier advances/materializes the
                // base first, then retires only the awaiting intents for which
                // this complete snapshot contains applicable evidence.
                let result = try applier.apply(
                    rows,
                    in: modelContext,
                    resolveRefreshCompletions: true
                )
                let summary = CollectionBatchApplySummary(
                    collectionIdentifier: collectionID,
                    sourceIdentifier: sourceID,
                    insertedCount: result.insertedCount,
                    updatedCount: result.updatedCount,
                    deletedCount: result.deletedCount
                )
                for key in result.resolvedStagedKeys {
                    tracer.record(
                        CollectionTraceEvent(
                            kind: .stagedInsertResolved,
                            collectionID: collectionID,
                            shapeID: sourceID,
                            modelName: configuration.modelName,
                            key: key,
                            message: "fetch adapter resolved staged insert",
                            metadata: [
                                "adapterOperation": "snapshotUpsert",
                                "outcome": "resolved",
                                "previousSyncState": String(describing: CollectionSyncState.stagedCreate),
                                "resultingSyncState": String(describing: CollectionSyncState.synced),
                            ]
                        )
                    )
                }
                for key in result.preservedStagedKeys {
                    tracer.record(
                        CollectionTraceEvent(
                            kind: .stagedDeletePreserved,
                            collectionID: collectionID,
                            shapeID: sourceID,
                            modelName: configuration.modelName,
                            key: key,
                            message: "fetch adapter preserved missing staged insert",
                            metadata: [
                                "adapterOperation": "snapshotMissing",
                                "outcome": "preservedStaged",
                                "previousSyncState": String(describing: CollectionSyncState.stagedCreate),
                                "resultingSyncState": String(describing: CollectionSyncState.stagedCreate),
                            ]
                        )
                    )
                }
                try onApply?(modelContext, summary)
            }
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
    let collectionID: String
    let modelName: String
    let missingRowPolicy: FetchMissingRowPolicy

    init(
        identifier: CollectionModelIdentifier<Model, ID>,
        rowDecoder: CollectionRowDecoder,
        collectionID: String? = nil,
        modelName: String,
        missingRowPolicy: FetchMissingRowPolicy
    ) {
        self.identifier = identifier
        self.rowDecoder = rowDecoder
        self.collectionID = collectionID ?? "\(modelName):fetch:\(modelName):all"
        self.modelName = modelName
        self.missingRowPolicy = missingRowPolicy
    }

    struct ApplyResult: Sendable, Hashable {
        let insertedCount: Int
        let updatedCount: Int
        let deletedCount: Int
        let resolvedStagedKeys: [String]
        let preservedStagedKeys: [String]
    }

    func apply(
        _ rows: [CollectionRow],
        in context: ModelContext,
        resolveRefreshCompletions: Bool = false
    ) throws -> ApplyResult {
        let existingModels = try context.fetch(FetchDescriptor<Model>())
        let unresolvedMutations = unresolvedMutationsByKey(in: context)
        let materializer = CollectionMaterializer(
            context: context,
            collectionID: collectionID,
            modelName: modelName,
            identifier: identifier,
            rowDecoder: rowDecoder
        )
        var returnedKeys = Set<String>()
        var insertedCount = 0
        var updatedCount = 0
        var deletedCount = 0
        var resolvedStagedKeys: [String] = []
        var preservedStagedKeys: [String] = []

        for row in rows {
            let decoded = try Model(collectionRow: row, decoder: rowDecoder)
            let key = identifier.key(for: decoded)
            guard key.isEmpty == false else {
                throw FetchCollectionError.missingStableIdentifier
            }

            returnedKeys.insert(key)
            let pending = unresolvedMutations[key] ?? []
            if pending.isEmpty,
               try CollectionStagedReconciler.applyUpsert(
                   key: key,
                   row: row,
                   mode: .replacement,
                   identifier: identifier,
                   rowDecoder: rowDecoder,
                   in: context
               ) == .resolved {
                updatedCount += 1
                resolvedStagedKeys.append(key)
                continue
            }

            let existed = try fetchModel(key: key, in: context) != nil
            try materializer.apply(.replacement(row), for: key)
            try materializer.materialize(key: key)
            if resolveRefreshCompletions {
                let resolved = resolveAwaitingMutations(
                    pending,
                    allowedOperations: [.create, .update]
                )
                if resolved {
                    // The first materialization above intentionally retained
                    // local intent while applying server evidence. Once the
                    // evidence is committed as completion, rebuild so the
                    // accepted server representation becomes visible.
                    try materializer.materialize(key: key)
                }
            }
            if try fetchModel(key: key, in: context) != nil {
                if existed {
                    updatedCount += 1
                } else {
                    insertedCount += 1
                }
            }
        }

        let existingKeys = Set(existingModels.map(identifier.key(for:)))
        let missingKeys = try existingKeys
            .union(materializer.baselineKeys())
            .subtracting(returnedKeys)

        for key in missingKeys {
            let pending = unresolvedMutations[key] ?? []
            if pending.isEmpty,
               try CollectionStagedReconciler.preserveDelete(
                   key: key,
                   identifier: identifier,
                   in: context
               ) == .preservedDelete {
                preservedStagedKeys.append(key)
                continue
            }

            switch missingRowPolicy {
            case .deleteSyncedRows:
                let existed = try fetchModel(key: key, in: context) != nil
                try materializer.apply(.absence, for: key)
                try materializer.materialize(key: key)
                if resolveRefreshCompletions {
                    let resolved = resolveAwaitingMutations(
                        pending,
                        allowedOperations: [.delete]
                    )
                    if resolved {
                        try materializer.materialize(key: key)
                    }
                }
                if existed, try fetchModel(key: key, in: context) == nil {
                    deletedCount += 1
                }
            case .keepLocalRows:
                if try materializer.baselineEvidence(for: key) != nil {
                    try materializer.invalidateBaseline(for: key)
                    try materializer.materialize(key: key)
                }
            }
        }

        try context.save()
        return ApplyResult(
            insertedCount: insertedCount,
            updatedCount: updatedCount,
            deletedCount: deletedCount,
            resolvedStagedKeys: resolvedStagedKeys,
            preservedStagedKeys: preservedStagedKeys
        )
    }

    private func fetchModel(key: String, in context: ModelContext) throws -> Model? {
        try context.fetch(identifier.fetchDescriptor(forSerializedKey: key)).first
    }

    private func unresolvedMutationsByKey(
        in context: ModelContext
    ) -> [String: [PendingCollectionMutation]] {
        let transactionIDs = Set(
            ((try? context.fetch(FetchDescriptor<PendingCollectionTransaction>())) ?? [])
                .filter { $0.collectionID == collectionID && $0.modelName == modelName }
                .map(\.id)
        )
        let mutations = ((try? context.fetch(FetchDescriptor<PendingCollectionMutation>())) ?? [])
            .filter { $0.modelName == modelName && transactionIDs.contains($0.transactionID) }
            .filter { $0.status.participatesInReconciliationOverlay }

        return Dictionary(
            grouping: mutations.sorted(by: mutationIsEarlier),
            by: \.targetKey
        )
    }

    @discardableResult
    private func resolveAwaitingMutations(
        _ mutations: [PendingCollectionMutation],
        allowedOperations: Set<CollectionMutationOperation>
    ) -> Bool {
        var resolved = false
        for mutation in mutations
        where mutation.status == .awaiting && allowedOperations.contains(mutation.operation) {
            mutation.status = .resolved
            mutation.errorMessage = nil
            resolved = true
        }
        return resolved
    }

    private func mutationIsEarlier(
        _ lhs: PendingCollectionMutation,
        _ rhs: PendingCollectionMutation
    ) -> Bool {
        if lhs.ordinal != rhs.ordinal {
            return lhs.ordinal < rhs.ordinal
        }
        if lhs.createdAt != rhs.createdAt {
            return lhs.createdAt < rhs.createdAt
        }
        return lhs.id.uuidString < rhs.id.uuidString
    }
}

public enum FetchCollectionError: Error, Sendable {
    case missingStableIdentifier
}
