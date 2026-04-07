import Foundation
import SwiftData

package protocol CollectionRuntime: Actor {
    func flushPendingMutations() async
    func reportAdapterApplied(
        sourceID: String,
        observedTokens: Set<String>,
        lastSyncedAt: Date?,
        offset: String?
    ) async
}

actor CollectionCoordinator<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: CollectionRuntime {
    private let collectionID: String
    private let configuration: CollectionOptions<Model, ID>
    private let sourceID: String
    private let adapterRuntime: any CollectionAdapterRuntime
    private let modelContainer: ModelContainer
    private let rowDecoder: CollectionRowDecoder
    private let debugLogger: CollectionDebugLogger
    private let writeTracer: CollectionWriteTracer
    private let queue: CollectionMutationQueue
    private let reconciler: CollectionMutationReconciler
    private let retryPolicy: any PendingMutationRetryDelaying
    private let commitSave: CollectionCommitSaver
    private let retrySleep: CollectionRetrySleeper

    private var bootstrapCompleted = false
    private var lifecycleState: CollectionLifecycleState = .idle
    private var liveTransactions: [UUID: CollectionTransaction] = [:]
    private var awaitedTransactionIDsByToken: [String: Set<UUID>] = [:]
    private var remainingTokensByTransactionID: [UUID: Set<String>] = [:]
    private var awaitingRefreshTransactionIDs: Set<UUID> = []
    private var pendingDispatchIDs: [UUID] = []
    private var isDrainingDispatch = false
    private var scheduledRetryAt: Date?
    private var scheduledRetryTask: Task<Void, Never>?
    private var debugEvents: [String] = []

    init(
        collectionID: String,
        configuration: CollectionOptions<Model, ID>,
        sourceID: String,
        adapterRuntime: any CollectionAdapterRuntime,
        modelContainer: ModelContainer,
        rowDecoder: CollectionRowDecoder,
        debugLogger: CollectionDebugLogger,
        writeTracer: CollectionWriteTracer,
        commitSave: @escaping CollectionCommitSaver = { try $0.save() },
        retryPolicy: any PendingMutationRetryDelaying = CollectionRetryPolicy(),
        retrySleep: @escaping CollectionRetrySleeper = defaultCollectionRetrySleep
    ) {
        self.collectionID = collectionID
        self.configuration = configuration
        self.sourceID = sourceID
        self.adapterRuntime = adapterRuntime
        self.modelContainer = modelContainer
        self.rowDecoder = rowDecoder
        self.debugLogger = debugLogger
        self.writeTracer = writeTracer
        self.queue = CollectionMutationQueue(modelContainer: modelContainer)
        self.reconciler = CollectionMutationReconciler(modelContainer: modelContainer)
        self.commitSave = commitSave
        self.retryPolicy = retryPolicy
        self.retrySleep = retrySleep
    }

    deinit {
        scheduledRetryTask?.cancel()
    }

    func bootstrapIfNeeded() async {
        guard bootstrapCompleted == false else { return }
        bootstrapCompleted = true
        lifecycleState = .bootstrapping
        await persistLifecycleState(errorMessage: nil)

        let pendingTransactions = queue.fetchAllPendingTransactions(collectionID: collectionID)
        debug("bootstrapping collection with \(pendingTransactions.count) persisted transactions")

        for transaction in pendingTransactions {
            if transaction.status == .sending {
                transaction.status = .pending
            }
            if transaction.status == .awaitingSync,
               let completion: CollectionMutationCompletion = transaction.completion() {
                switch completion {
                case .awaitTokens(let tokens):
                    register(transactionID: transaction.id, awaiting: tokens)
                case .refresh:
                    awaitingRefreshTransactionIDs.insert(transaction.id)
                case .immediate:
                    reconciler.resolveTransaction(id: transaction.id, collectionID: collectionID)
                }
            }
        }

        try? queue.saveContext()
        refreshPendingModelStates()
        await drainDispatchIfNeeded()
        lifecycleState = .idle
        await persistLifecycleState(errorMessage: nil)
    }

    func start() async {
        await bootstrapIfNeeded()
        lifecycleState = .syncing
        await persistLifecycleState(errorMessage: nil)
        await adapterRuntime.start()
        await drainDispatchIfNeeded()
    }

    func stop() async {
        cancelScheduledRetry()
        await adapterRuntime.stop()
        lifecycleState = .idle
        await persistLifecycleState(errorMessage: nil)
    }

    func refresh() async {
        await bootstrapIfNeeded()
        lifecycleState = .syncing
        await persistLifecycleState(errorMessage: nil)
        await adapterRuntime.refresh()
        await drainDispatchIfNeeded()
    }

    func status() -> CollectionLifecycleState {
        lifecycleState
    }

    func flushPendingMutations() async {
        await bootstrapIfNeeded()
        await drainDispatchIfNeeded()
    }

    func reportAdapterApplied(
        sourceID: String,
        observedTokens: Set<String>,
        lastSyncedAt: Date?,
        offset: String?
    ) async {
        guard sourceID == self.sourceID else { return }
        await didApplyFromAdapter(
            observedTokens: observedTokens,
            lastSyncedAt: lastSyncedAt,
            offset: offset
        )
    }

    func insert(
        _ build: @escaping @Sendable () throws -> Model,
        metadata: [String: CollectionValue]
    ) async throws -> CollectionTransaction {
        try await transaction { builder in
            try builder.insert(build, metadata: metadata)
        }
    }

    func update(
        _ key: ID,
        metadata: [String: CollectionValue],
        _ mutate: @escaping @Sendable (Model) throws -> Void
    ) async throws -> CollectionTransaction {
        try await transaction { builder in
            try builder.update(key, metadata: metadata, mutate)
        }
    }

    func delete(
        _ key: ID,
        metadata: [String: CollectionValue]
    ) async throws -> CollectionTransaction {
        try await transaction { builder in
            try builder.delete(key, metadata: metadata)
        }
    }

    func transaction(
        _ body: @escaping @Sendable (CollectionTransactionBuilder<Model, ID>) throws -> Void
    ) async throws -> CollectionTransaction {
        await bootstrapIfNeeded()

        let liveTransaction = CollectionTransaction(collectionID: collectionID)
        liveTransactions[liveTransaction.id] = liveTransaction
        trace(
            .transactionStarted,
            transactionID: liveTransaction.id,
            message: "created collection transaction"
        )

        let builder = CollectionTransactionBuilder(
            modelContainer: modelContainer,
            transactionID: liveTransaction.id,
            collectionID: collectionID,
            shapeID: sourceID,
            modelName: configuration.modelName,
            identifier: configuration.identifier,
            rowDecoder: rowDecoder,
            writeTracer: writeTracer
        )

        do {
            try body(builder)
            let preparedTransaction = builder.preparedTransaction()

            if preparedTransaction.isEmpty {
                trace(
                    .transactionCompleted,
                    transactionID: liveTransaction.id,
                    message: "transaction completed without persisted mutations"
                )
                await liveTransaction.complete()
                liveTransactions.removeValue(forKey: liveTransaction.id)
                return liveTransaction
            }

            let sequenceNumber = queue.nextTransactionSequenceNumber(collectionID: collectionID)
            let pendingTransaction = PendingCollectionTransaction(
                id: liveTransaction.id,
                collectionID: collectionID,
                shapeID: sourceID,
                modelName: configuration.modelName,
                sequenceNumber: sequenceNumber,
                status: .pending
            )
            let persistedMutations = try preparedTransaction.persistedMutations()
            try commitPreparedTransaction(
                preparedTransaction,
                pendingTransaction: pendingTransaction,
                persistedMutations: persistedMutations
            )
            await liveTransaction.markDurablyQueued()
            trace(
                .transactionPersisted,
                transactionID: liveTransaction.id,
                sequenceNumber: sequenceNumber,
                pendingMutationCount: preparedTransaction.mutations.count,
                message: "persisted transaction to durable outbox"
            )

            trace(
                .dispatchEnqueued,
                transactionID: liveTransaction.id,
                sequenceNumber: sequenceNumber,
                pendingMutationCount: preparedTransaction.mutations.count,
                message: "enqueued transaction for dispatch"
            )
            await enqueueDispatch(ids: [liveTransaction.id])
            return liveTransaction
        } catch {
            liveTransactions.removeValue(forKey: liveTransaction.id)
            trace(
                .transactionFailed,
                transactionID: liveTransaction.id,
                message: "transaction failed before dispatch",
                error: error
            )
            throw error
        }
    }

    func didApplyFromAdapter(
        observedTokens: Set<String>,
        lastSyncedAt: Date?,
        offset: String?
    ) async {
        trace(
            .shapeBatchApplied,
            observedTokens: observedTokens.map(tokenString).sorted(),
            offset: offset,
            message: "applied incoming adapter batch"
        )
        if observedTokens.isEmpty == false {
            debug("observed tokens \(observedTokens.map(tokenString).sorted()) for source \(sourceID)")
        }

        let completedTransactionIDs = reconciler.resolveTransactions(
            observedTokens: observedTokens,
            collectionID: collectionID,
            remainingTokensByTransactionID: &remainingTokensByTransactionID,
            awaitedTransactionIDsByToken: &awaitedTransactionIDsByToken
        )
        for transactionID in completedTransactionIDs {
            if let liveTransaction = liveTransactions.removeValue(forKey: transactionID) {
                await liveTransaction.complete()
            }
            trace(
                .transactionCompleted,
                transactionID: transactionID,
                observedTokens: observedTokens.map(tokenString).sorted(),
                message: "completed transaction after reconciliation"
            )
        }

        lifecycleState = .ready
        await persistLifecycleState(errorMessage: nil, lastSyncedAt: lastSyncedAt)
        await drainDispatchIfNeeded()
    }

    func didRefreshComplete(lastSyncedAt: Date?) async {
        let transactionIDs = awaitingRefreshTransactionIDs
        awaitingRefreshTransactionIDs.removeAll()

        for transactionID in transactionIDs {
            reconciler.resolveTransaction(id: transactionID, collectionID: collectionID)
            if let liveTransaction = liveTransactions.removeValue(forKey: transactionID) {
                await liveTransaction.complete()
            }
            trace(
                .transactionCompleted,
                transactionID: transactionID,
                message: "completed transaction after refresh confirmation"
            )
        }

        refreshPendingModelStates()
        lifecycleState = .ready
        await persistLifecycleState(errorMessage: nil, lastSyncedAt: lastSyncedAt)
    }

    func didEncounterAdapterError(_ error: Error) async {
        lifecycleState = .error(String(describing: error))
        await persistLifecycleState(errorMessage: String(describing: error))
        debug("adapter error for \(configuration.debugName): \(error)")
    }

    func recordedDebugEvents() -> [String] {
        debugEvents
    }

    private func enqueueDispatch(ids: [UUID]) async {
        if ids.isEmpty == false {
            cancelScheduledRetry()
        }
        for id in ids where pendingDispatchIDs.contains(id) == false {
            pendingDispatchIDs.append(id)
        }
        await drainDispatchIfNeeded()
    }

    private func drainDispatchIfNeeded() async {
        guard isDrainingDispatch == false else { return }
        isDrainingDispatch = true
        defer { isDrainingDispatch = false }

        while true {
            while pendingDispatchIDs.isEmpty == false {
                let id = pendingDispatchIDs.removeFirst()
                await processPendingTransaction(id: id)
            }

            let eligibleIDs = queue.eligibleDispatchTransactionIDs(
                collectionID: collectionID,
                now: Date()
            )
            .filter { pendingDispatchIDs.contains($0) == false }

            guard eligibleIDs.isEmpty == false else {
                scheduleNextRetryIfNeeded()
                break
            }

            cancelScheduledRetry()
            lifecycleState = .replaying
            await persistLifecycleState(errorMessage: nil)
            pendingDispatchIDs.append(contentsOf: eligibleIDs)
        }
    }

    private func processPendingTransaction(id: UUID) async {
        let transactionRecord = queue.fetchPendingTransaction(id: id, collectionID: collectionID)
        guard let transactionRecord else { return }
        guard transactionRecord.status == .pending || transactionRecord.status == .failed else { return }
        guard transactionRecord.nextRetryAt.map({ $0 <= Date() }) ?? true else { return }

        let pendingMutations = queue.fetchPendingMutations(transactionID: id)
        guard pendingMutations.isEmpty == false else { return }

        transactionRecord.status = .sending
        transactionRecord.recordAttempt()
        transactionRecord.lastErrorMessage = nil
        transactionRecord.nextRetryAt = nil
        for mutation in pendingMutations {
            mutation.status = .sending
            mutation.recordAttempt()
            mutation.errorMessage = nil
            mutation.nextRetryAt = nil
        }
        try? queue.saveContext()
        refreshPendingModelStates(keys: Set(pendingMutations.map(\.targetKey)))

        let transaction = liveTransactions[id] ?? CollectionTransaction(id: id, collectionID: collectionID)
        liveTransactions[id] = transaction
        await transaction.markSending()
        trace(
            .dispatchStarted,
            transactionID: id,
            sequenceNumber: transactionRecord.sequenceNumber,
            attemptCount: transactionRecord.attemptCount,
            pendingMutationCount: pendingMutations.count,
            message: "dispatching queued transaction"
        )

        do {
            let completion = try await dispatchMutationGroups(
                transaction: transaction,
                pendingMutations: pendingMutations
            )

            switch completion {
            case .immediate:
                let immediateCompletion: CollectionMutationCompletion = .immediate
                transactionRecord.setCompletion(immediateCompletion)
                reconciler.resolveTransaction(id: id, collectionID: collectionID)
                try queue.saveContext()
                refreshPendingModelStates(keys: Set(pendingMutations.map(\.targetKey)))
                if let liveTransaction = liveTransactions.removeValue(forKey: id) {
                    await liveTransaction.complete()
                }
                trace(
                    .transactionCompleted,
                    transactionID: id,
                    sequenceNumber: transactionRecord.sequenceNumber,
                    attemptCount: transactionRecord.attemptCount,
                    message: "completed transaction immediately"
                )

            case .awaitTokens(let tokens):
                guard tokens.isEmpty == false else {
                    throw CollectionError.missingAwaitedObservationTokens
                }

                transactionRecord.setCompletion(.awaitTokens(tokens))
                transactionRecord.status = .awaitingSync
                transactionRecord.lastErrorMessage = nil
                for mutation in pendingMutations {
                    mutation.status = .awaitingSync
                    mutation.errorMessage = nil
                }
                try queue.saveContext()
                refreshPendingModelStates(keys: Set(pendingMutations.map(\.targetKey)))

                register(transactionID: id, awaiting: tokens)
                await transaction.markAwaitingSync()
                trace(
                    .awaitingSync,
                    transactionID: id,
                    sequenceNumber: transactionRecord.sequenceNumber,
                    attemptCount: transactionRecord.attemptCount,
                    awaitedTokens: tokens.map(tokenString).sorted(),
                    pendingMutationCount: pendingMutations.count,
                    message: "awaiting observation tokens from adapter"
                )

            case .refresh:
                let refreshCompletion: CollectionMutationCompletion = .refresh
                transactionRecord.setCompletion(refreshCompletion)
                transactionRecord.status = .awaitingSync
                transactionRecord.lastErrorMessage = nil
                for mutation in pendingMutations {
                    mutation.status = .awaitingSync
                    mutation.errorMessage = nil
                }
                try queue.saveContext()
                refreshPendingModelStates(keys: Set(pendingMutations.map(\.targetKey)))
                awaitingRefreshTransactionIDs.insert(id)
                await transaction.markAwaitingSync()
                trace(
                    .awaitingSync,
                    transactionID: id,
                    sequenceNumber: transactionRecord.sequenceNumber,
                    attemptCount: transactionRecord.attemptCount,
                    pendingMutationCount: pendingMutations.count,
                    message: "awaiting adapter refresh completion"
                )
            }
        } catch {
            transactionRecord.markFailed(error, retryPolicy: retryPolicy)
            for mutation in pendingMutations {
                mutation.markFailed(error, retryPolicy: retryPolicy)
            }
            try? queue.saveContext()

            refreshPendingModelStates(keys: Set(pendingMutations.map(\.targetKey)))

            if let liveTransaction = liveTransactions.removeValue(forKey: id) {
                await liveTransaction.fail(error)
            }

            lifecycleState = .error(String(describing: error))
            await persistLifecycleState(errorMessage: String(describing: error))
            trace(
                .transactionFailed,
                transactionID: id,
                sequenceNumber: transactionRecord.sequenceNumber,
                attemptCount: transactionRecord.attemptCount,
                pendingMutationCount: pendingMutations.count,
                message: "dispatch failed",
                error: error
            )
            debug("failed dispatch for \(configuration.debugName) transaction \(id): \(error)")
        }
    }

    private func scheduleNextRetryIfNeeded(now: Date = Date()) {
        let nextRetryAt = queue.nextRetryAt(collectionID: collectionID, now: now)
        guard let nextRetryAt else {
            cancelScheduledRetry()
            return
        }
        guard scheduledRetryAt != nextRetryAt else { return }

        cancelScheduledRetry()
        scheduledRetryAt = nextRetryAt
        let delay = max(0, nextRetryAt.timeIntervalSince(now))
        let retrySleep = self.retrySleep
        scheduledRetryTask = Task {
            await retrySleep(delay)
            guard Task.isCancelled == false else { return }
            await self.scheduledRetryDidFire(expectedRetryAt: nextRetryAt)
        }
    }

    private func scheduledRetryDidFire(expectedRetryAt: Date) async {
        guard scheduledRetryAt == expectedRetryAt else { return }
        scheduledRetryAt = nil
        scheduledRetryTask = nil
        await drainDispatchIfNeeded()
    }

    private func cancelScheduledRetry() {
        scheduledRetryTask?.cancel()
        scheduledRetryTask = nil
        scheduledRetryAt = nil
    }

    private func commitPreparedTransaction(
        _ preparedTransaction: PreparedCollectionTransaction,
        pendingTransaction: PendingCollectionTransaction,
        persistedMutations: [PendingCollectionMutation]
    ) throws {
        let context = ModelContext(modelContainer)
        context.insert(pendingTransaction)
        for mutation in persistedMutations {
            context.insert(mutation)
        }

        try applyOptimisticChanges(
            preparedTransaction.optimisticChanges,
            in: context
        )

        for key in preparedTransaction.touchedKeys {
            try CollectionMutationReconciler.refreshModelState(
                for: Model.self,
                key: key,
                modelName: configuration.modelName,
                identifier: configuration.identifier,
                in: context
            )
        }

        try commitSave(context)

        try traceCommittedOptimisticChanges(
            preparedTransaction.mutations,
            transactionID: preparedTransaction.transactionID,
            in: context
        )
    }

    private func dispatchMutationGroups(
        transaction: CollectionTransaction,
        pendingMutations: [PendingCollectionMutation]
    ) async throws -> CollectionMutationCompletion {
        var awaitedTokens = Set<String>()
        var requiresRefresh = false

        for group in CollectionMutationDispatcher.groups(from: pendingMutations) {
            let mutations = group.map(makeCollectionMutation(from:))
            let context = CollectionMutationContext<Model, ID>(transaction: transaction, mutations: mutations)
            trace(
                .handlerInvoked,
                transactionID: transaction.id,
                key: group.first?.targetKey,
                operation: group.first?.operation,
                pendingMutationCount: group.count,
                message: "invoking outbound mutation handler"
            )

            let completion: CollectionMutationCompletion
            switch group[0].operation {
            case .create:
                guard let handler = configuration.onInsert else {
                    throw CollectionError.missingMutationHandler(.create)
                }
                completion = try await handler(context)
            case .update:
                guard let handler = configuration.onUpdate else {
                    throw CollectionError.missingMutationHandler(.update)
                }
                completion = try await handler(context)
            case .delete:
                guard let handler = configuration.onDelete else {
                    throw CollectionError.missingMutationHandler(.delete)
                }
                completion = try await handler(context)
            }

            switch completion {
            case .immediate:
                trace(
                    .handlerReturned,
                    transactionID: transaction.id,
                    key: group.first?.targetKey,
                    operation: group.first?.operation,
                    pendingMutationCount: group.count,
                    message: "outbound handler completed immediately"
                )
            case .refresh:
                requiresRefresh = true
                trace(
                    .handlerReturned,
                    transactionID: transaction.id,
                    key: group.first?.targetKey,
                    operation: group.first?.operation,
                    pendingMutationCount: group.count,
                    message: "outbound handler requested refresh completion"
                )
            case .awaitTokens(let tokens):
                guard tokens.isEmpty == false else {
                    throw CollectionError.missingAwaitedObservationTokens
                }
                awaitedTokens.formUnion(tokens)
                trace(
                    .handlerReturned,
                    transactionID: transaction.id,
                    key: group.first?.targetKey,
                    operation: group.first?.operation,
                    awaitedTokens: tokens.map(tokenString).sorted(),
                    pendingMutationCount: group.count,
                    message: "outbound handler returned awaited observation tokens"
                )
            }
        }

        if requiresRefresh {
            return .refresh
        }
        if awaitedTokens.isEmpty == false {
            return .awaitTokens(awaitedTokens)
        }
        return .immediate
    }

    private func applyOptimisticChanges(
        _ changes: [OptimisticModelChange],
        in context: ModelContext
    ) throws {
        let mutationDate = Date()
        for change in changes {
            switch change {
            case .create(_, let row):
                let model = try Model(collectionRow: row, decoder: rowDecoder)
                model.collectionLastLocalMutationAt = mutationDate
                context.insert(model)
            case .update(let key, let row):
                guard let model = try fetchModel(key: key, in: context) else {
                    throw CollectionError.modelNotFound(key)
                }
                try model.apply(collectionRow: row, decoder: rowDecoder)
                model.collectionLastLocalMutationAt = mutationDate
            case .delete(let key):
                guard let model = try fetchModel(key: key, in: context) else {
                    throw CollectionError.modelNotFound(key)
                }
                model.collectionSyncState = .pendingDelete
                model.collectionLastLocalMutationAt = mutationDate
            }
        }
    }

    private func traceCommittedOptimisticChanges(
        _ mutations: [CollectionMutation],
        transactionID: UUID,
        in context: ModelContext
    ) throws {
        for mutation in mutations {
            let pendingMutationCount = try fetchModel(key: mutation.key, in: context)?.collectionPendingMutationCount
            let message = switch mutation.operation {
            case .create:
                "applied optimistic insert"
            case .update:
                "applied optimistic update"
            case .delete:
                "marked row pending delete"
            }
            trace(
                .optimisticMutationRecorded,
                transactionID: transactionID,
                key: mutation.key,
                operation: mutation.operation,
                pendingMutationCount: pendingMutationCount,
                message: message
            )
        }
    }

    private func fetchModel(key: String, in context: ModelContext) throws -> Model? {
        try context.fetch(configuration.identifier.fetchDescriptor(forSerializedKey: key)).first
    }

    private func register(transactionID: UUID, awaiting tokens: Set<String>) {
        guard tokens.isEmpty == false else { return }
        remainingTokensByTransactionID[transactionID] = tokens
        for token in tokens {
            awaitedTransactionIDsByToken[token, default: []].insert(transactionID)
        }
    }

    private func refreshPendingModelStates(keys: Set<String>? = nil) {
        let keysToRefresh: Set<String>
        if let keys {
            keysToRefresh = keys
        } else {
            keysToRefresh = Set(
                queue.fetchAllPendingMutations(collectionID: collectionID)
                    .map(\.targetKey)
            )
        }

        for key in keysToRefresh {
            try? CollectionMutationReconciler.refreshModelState(
                for: Model.self,
                key: key,
                modelName: configuration.modelName,
                identifier: configuration.identifier,
                in: queue.context
            )
        }
        try? queue.saveContext()
    }

    private func persistLifecycleState(
        errorMessage: String?,
        lastSyncedAt: Date? = nil
    ) async {
        let metadata = queue.fetchOrCreateCollectionMetadata(
            collectionID: collectionID,
            shapeID: sourceID,
            modelName: configuration.modelName,
            debugName: configuration.debugName
        )
        metadata.status = lifecycleState
        metadata.lastErrorMessage = errorMessage
        if case .replaying = lifecycleState {
            metadata.lastReplayAt = Date()
        }
        if let lastSyncedAt {
            metadata.lastSyncedAt = lastSyncedAt
        }
        try? queue.saveContext()
    }

    private func debug(_ message: String) {
        debugEvents.append(message)
        debugLogger.log(.debug, category: "CollectionCoordinator", message: message)
    }

    private func tokenString(_ token: String) -> String {
        String(describing: token)
    }

    private func trace(
        _ kind: CollectionWriteDebugEventKind,
        transactionID: UUID? = nil,
        key: String? = nil,
        operation: CollectionMutationOperation? = nil,
        sequenceNumber: Int? = nil,
        attemptCount: Int? = nil,
        awaitedTokens: [String] = [],
        observedTokens: [String] = [],
        resolvedTransactionIDs: [UUID] = [],
        offset: String? = nil,
        pendingMutationCount: Int? = nil,
        message: String? = nil,
        error: Error? = nil,
        metadata: [String: String] = [:]
    ) {
        writeTracer.record(
            CollectionWriteDebugEvent(
                kind: kind,
                collectionID: collectionID,
                shapeID: sourceID,
                modelName: configuration.modelName,
                transactionID: transactionID,
                key: key,
                operation: operation,
                sequenceNumber: sequenceNumber,
                attemptCount: attemptCount,
                awaitedTokens: awaitedTokens,
                observedTokens: observedTokens,
                resolvedTransactionIDs: resolvedTransactionIDs,
                offset: offset,
                pendingMutationCount: pendingMutationCount,
                message: message,
                errorDescription: error.map(String.init(describing:)),
                metadata: metadata
            )
        )
    }

    private func makeCollectionMutation(from pending: PendingCollectionMutation) -> CollectionMutation {
        let original = pending.originalRow
        let modified: CollectionRow?
        switch pending.operation {
        case .create, .update:
            modified = pending.payload
        case .delete:
            modified = nil
        }

        let changes = pending.payload.reduce(into: CollectionRow()) { partialResult, entry in
            if pending.changedFields.contains(entry.key) {
                partialResult[entry.key] = entry.value
            }
        }

        return CollectionMutation(
            operation: pending.operation,
            key: pending.targetKey,
            original: original,
            modified: modified,
            changes: changes,
            metadata: pending.metadata
        )
    }
}
