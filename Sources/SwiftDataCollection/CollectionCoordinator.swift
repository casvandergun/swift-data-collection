import Foundation
import SwiftData

package protocol CollectionRuntime: Actor {
    func flush() async
    func setConnectivityState(_ state: CollectionConnectivityState) async
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
    private let tracer: CollectionTracer
    private let writeGate: CollectionWriteGate
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
    private var connectivityState: CollectionConnectivityState
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
        tracer: CollectionTracer,
        writeGate: CollectionWriteGate = CollectionWriteGate(),
        commitSave: @escaping CollectionCommitSaver = { try $0.save() },
        retryPolicy: any PendingMutationRetryDelaying = CollectionRetryPolicy(),
        retrySleep: @escaping CollectionRetrySleeper = defaultCollectionRetrySleep,
        connectivityState: CollectionConnectivityState = .online
    ) {
        self.collectionID = collectionID
        self.configuration = configuration
        self.sourceID = sourceID
        self.adapterRuntime = adapterRuntime
        self.modelContainer = modelContainer
        self.rowDecoder = rowDecoder
        self.debugLogger = debugLogger
        self.tracer = tracer
        self.writeGate = writeGate
        self.queue = CollectionMutationQueue(modelContainer: modelContainer)
        self.reconciler = CollectionMutationReconciler(modelContainer: modelContainer)
        self.commitSave = commitSave
        self.retryPolicy = retryPolicy
        self.retrySleep = retrySleep
        self.connectivityState = connectivityState
    }

    deinit {
        scheduledRetryTask?.cancel()
    }

    func bootstrapIfNeeded() async {
        guard bootstrapCompleted == false else { return }
        bootstrapCompleted = true
        await transitionLifecycle(to: .bootstrapping, reason: "bootstrap started", errorMessage: nil)

        let pendingTransactions = queue.fetchAllPendingTransactions(collectionID: collectionID)
        trace(
            .bootstrapStarted,
            pendingMutationCount: queue.fetchAllPendingMutations(collectionID: collectionID).count,
            message: "bootstrapping collection with persisted outbox",
            metadata: [
                "transactionCount": String(pendingTransactions.count),
                "transactions": pendingTransactions.map(transactionDebugSummary).joined(separator: " | "),
            ]
        )
        debug("bootstrapping collection with \(pendingTransactions.count) persisted transactions")

        for transaction in pendingTransactions {
            if transaction.status == .sending {
                trace(
                    .replayScheduled,
                    transactionID: transaction.id,
                    sequenceNumber: transaction.sequenceNumber,
                    attemptCount: transaction.attemptCount,
                    message: "reset persisted sending transaction for replay",
                    metadata: transactionTraceMetadata(transaction)
                )
                transaction.status = .pending
            }
            if transaction.status == .awaitingSync,
               let completion: CollectionMutationCompletion = transaction.completion() {
                switch completion {
                case .awaitTokens(let tokens):
                    register(transactionID: transaction.id, awaiting: tokens)
                    trace(
                        .awaitedTokensRegistered,
                        transactionID: transaction.id,
                        sequenceNumber: transaction.sequenceNumber,
                        attemptCount: transaction.attemptCount,
                        awaitedTokens: tokens.map(tokenString).sorted(),
                        message: "re-registered awaited observation tokens during bootstrap",
                        metadata: transactionTraceMetadata(transaction)
                    )
                case .refresh:
                    awaitingRefreshTransactionIDs.insert(transaction.id)
                    trace(
                        .awaitedTokensRegistered,
                        transactionID: transaction.id,
                        sequenceNumber: transaction.sequenceNumber,
                        attemptCount: transaction.attemptCount,
                        message: "re-registered refresh completion during bootstrap",
                        metadata: transactionTraceMetadata(transaction)
                    )
                case .immediate:
                    writeGate.withCriticalSection {
                        reconciler.resolveTransaction(id: transaction.id, collectionID: collectionID)
                    }
                }
            }
        }

        try? saveQueueContext()
        refreshPendingModelStates()
        await drainDispatchIfNeeded()
        trace(
            .bootstrapCompleted,
            pendingMutationCount: queue.fetchAllPendingMutations(collectionID: collectionID).count,
            message: "completed collection bootstrap"
        )
        await transitionLifecycle(to: .idle, reason: "bootstrap completed", errorMessage: nil)
    }

    func start() async {
        await bootstrapIfNeeded()
        await transitionLifecycle(to: .syncing, reason: "collection start requested", errorMessage: nil)
        await adapterRuntime.start()
        await drainDispatchIfNeeded()
    }

    func stop() async {
        cancelScheduledRetry()
        await adapterRuntime.stop()
        await transitionLifecycle(to: .idle, reason: "collection stop requested", errorMessage: nil)
    }

    func refresh() async {
        await bootstrapIfNeeded()
        await transitionLifecycle(to: .syncing, reason: "collection refresh requested", errorMessage: nil)
        await adapterRuntime.refresh()
        await drainDispatchIfNeeded()
    }

    func status() -> CollectionLifecycleState {
        lifecycleState
    }

    func flush() async {
        await bootstrapIfNeeded()
        await drainDispatchIfNeeded()
    }

    func setConnectivityState(_ state: CollectionConnectivityState) async {
        guard connectivityState != state else { return }
        let oldState = connectivityState
        connectivityState = state
        trace(
            .connectivityChanged,
            message: "collection connectivity changed",
            metadata: [
                "from": oldState.rawValue,
                "to": state.rawValue,
                "connectivity": state.rawValue,
            ]
        )

        switch state {
        case .offline:
            cancelScheduledRetry()
            await transitionLifecycle(to: .offline, reason: "connectivity changed offline", errorMessage: nil)
        case .online:
            makeFailedTransactionsEligibleForReconnectRetry()
            await transitionLifecycle(to: .syncing, reason: "connectivity changed online", errorMessage: nil)
            trace(
                .dispatchResumedOnline,
                message: "resuming dispatch after connectivity returned",
                metadata: ["connectivity": state.rawValue]
            )
            await drainDispatchIfNeeded()
        }
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

    func stageInsert(
        _ build: @escaping @Sendable () throws -> Model
    ) async throws -> StagedInsertOutcome {
        await bootstrapIfNeeded()
        let proposed = try build()
        let key = configuration.identifier.key(for: proposed)
        guard key.isEmpty == false else {
            throw CollectionError.missingStableIdentifier
        }

        let outcome = try writeGate.withCriticalSection {
            let context = ModelContext(modelContainer)
            let pending = CollectionMutationReconciler.unresolvedMutations(
                modelName: configuration.modelName,
                targetKey: key,
                in: context
            )
            guard pending.isEmpty else {
                throw CollectionError.stagedOperationHasPendingMutations(key: key, count: pending.count)
            }
            if let existing = try fetchModel(key: key, in: context) {
                switch existing.collectionSyncState {
                case .stagedCreate:
                    return StagedInsertOutcome.alreadyStaged
                case .synced:
                    return StagedInsertOutcome.alreadySynced
                default:
                    throw CollectionError.invalidStagedTransition(
                        key: key,
                        state: existing.collectionSyncState
                    )
                }
            }

            proposed.collectionSyncState = .stagedCreate
            proposed.collectionPendingMutationCount = 0
            context.insert(proposed)
            try commitSave(context)
            return StagedInsertOutcome.inserted
        }

        let previousSyncState: String = switch outcome {
        case .inserted: "absent"
        case .alreadyStaged: String(describing: CollectionSyncState.stagedCreate)
        case .alreadySynced: String(describing: CollectionSyncState.synced)
        }
        let resultingSyncState: String = switch outcome {
        case .inserted, .alreadyStaged: String(describing: CollectionSyncState.stagedCreate)
        case .alreadySynced: String(describing: CollectionSyncState.synced)
        }
        trace(
            outcome == .inserted ? .stagedInsertCreated : .stagedInsertNoOp,
            key: key,
            pendingMutationCount: 0,
            message: outcome == .inserted ? "persisted staged insert" : "staged insert was idempotent",
            metadata: [
                "outcome": String(describing: outcome),
                "previousSyncState": previousSyncState,
                "resultingSyncState": resultingSyncState,
                "raceOutcome": outcome == .alreadySynced ? "adapterWon" : "none",
            ]
        )
        return outcome
    }

    func updateStaged(
        _ key: ID,
        _ mutate: @escaping @Sendable (Model) throws -> Void
    ) async throws {
        await bootstrapIfNeeded()
        let serializedKey = configuration.identifier.serialize(key)
        guard serializedKey.isEmpty == false else {
            throw CollectionError.missingStableIdentifier
        }
        try writeGate.withCriticalSection {
            let context = ModelContext(modelContainer)
            let pending = CollectionMutationReconciler.unresolvedMutations(
                modelName: configuration.modelName,
                targetKey: serializedKey,
                in: context
            )
            guard pending.isEmpty else {
                throw CollectionError.stagedOperationHasPendingMutations(
                    key: serializedKey,
                    count: pending.count
                )
            }
            guard let model = try fetchModel(key: serializedKey, in: context) else {
                throw CollectionError.modelNotFound(serializedKey)
            }
            guard model.collectionSyncState == .stagedCreate else {
                throw CollectionError.invalidStagedTransition(
                    key: serializedKey,
                    state: model.collectionSyncState
                )
            }
            try mutate(model)
            let actualKey = configuration.identifier.key(for: model)
            guard actualKey == serializedKey else {
                throw CollectionError.stableIdentifierChanged(
                    expected: serializedKey,
                    actual: actualKey
                )
            }
            model.collectionSyncState = .stagedCreate
            model.collectionPendingMutationCount = 0
            try commitSave(context)
        }
        trace(
            .stagedInsertUpdated,
            key: serializedKey,
            pendingMutationCount: 0,
            message: "updated staged insert",
            metadata: [
                "previousSyncState": String(describing: CollectionSyncState.stagedCreate),
                "resultingSyncState": String(describing: CollectionSyncState.stagedCreate),
                "outcome": "updated",
            ]
        )
    }

    func publishStagedInsert(
        _ key: ID,
        metadata: [String: CollectionValue]
    ) async throws -> CollectionTransaction {
        try await performTransaction(promotingExistingCreate: true) { builder in
            try builder.insertExisting(key, metadata: metadata)
        }
    }

    func discardStagedInsert(_ key: ID) async throws {
        await bootstrapIfNeeded()
        let serializedKey = configuration.identifier.serialize(key)
        guard serializedKey.isEmpty == false else {
            throw CollectionError.missingStableIdentifier
        }
        try writeGate.withCriticalSection {
            let context = ModelContext(modelContainer)
            let pending = CollectionMutationReconciler.unresolvedMutations(
                modelName: configuration.modelName,
                targetKey: serializedKey,
                in: context
            )
            guard pending.isEmpty else {
                throw CollectionError.stagedOperationHasPendingMutations(
                    key: serializedKey,
                    count: pending.count
                )
            }
            guard let model = try fetchModel(key: serializedKey, in: context) else {
                throw CollectionError.modelNotFound(serializedKey)
            }
            guard model.collectionSyncState == .stagedCreate else {
                throw CollectionError.invalidStagedTransition(
                    key: serializedKey,
                    state: model.collectionSyncState
                )
            }
            context.delete(model)
            try commitSave(context)
        }
        trace(
            .stagedInsertDiscarded,
            key: serializedKey,
            pendingMutationCount: 0,
            message: "discarded staged insert",
            metadata: [
                "previousSyncState": String(describing: CollectionSyncState.stagedCreate),
                "resultingSyncState": "absent",
                "outcome": "discarded",
            ]
        )
    }

    func transaction(
        _ body: @escaping @Sendable (CollectionTransactionBuilder<Model, ID>) throws -> Void
    ) async throws -> CollectionTransaction {
        try await performTransaction(promotingExistingCreate: false, body)
    }

    private func performTransaction(
        promotingExistingCreate: Bool,
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
            tracer: tracer
        )

        do {
            try body(builder)
            let preparedTransaction = builder.preparedTransaction(
                promotingExistingCreate: promotingExistingCreate
            )

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
            if promotingExistingCreate {
                trace(
                    .stagedInsertPublished,
                    transactionID: liveTransaction.id,
                    key: preparedTransaction.touchedKeys.first,
                    pendingMutationCount: preparedTransaction.mutations.count,
                    message: "published staged insert",
                    metadata: [
                        "previousSyncState": String(describing: CollectionSyncState.stagedCreate),
                        "resultingSyncState": String(describing: CollectionSyncState.pendingCreate),
                        "outcome": "published",
                    ]
                )
            }

            trace(
                .dispatchEnqueued,
                transactionID: liveTransaction.id,
                sequenceNumber: sequenceNumber,
                pendingMutationCount: preparedTransaction.mutations.count,
                message: "enqueued transaction for dispatch"
            )
            switch configuration.dispatchWait {
            case .durablyQueued:
                enqueueDispatchWithoutWaiting(ids: [liveTransaction.id])
            case .dispatchAttempted:
                await enqueueDispatch(ids: [liveTransaction.id])
            }
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
            .adapterBatchObserved,
            observedTokens: observedTokens.map(tokenString).sorted(),
            offset: offset,
            message: "coordinator observed applied adapter batch",
            metadata: [
                "observedTXIDs": observedTokens.map(tokenString).sorted().joined(separator: ","),
                "awaitingTransactions": String(remainingTokensByTransactionID.count),
            ]
        )
        trace(
            .shapeBatchApplied,
            observedTokens: observedTokens.map(tokenString).sorted(),
            offset: offset,
            message: "applied incoming adapter batch"
        )
        if observedTokens.isEmpty == false {
            debug("observed tokens \(observedTokens.map(tokenString).sorted()) for source \(sourceID)")
        }

        let completedTransactionIDs = writeGate.withCriticalSection {
            reconciler.resolveTransactions(
                observedTokens: observedTokens,
                collectionID: collectionID,
                remainingTokensByTransactionID: &remainingTokensByTransactionID,
                awaitedTransactionIDsByToken: &awaitedTransactionIDsByToken
            )
        }
        for transactionID in completedTransactionIDs {
            if let liveTransaction = liveTransactions.removeValue(forKey: transactionID) {
                await liveTransaction.complete()
            }
            trace(
                .transactionCompleted,
                transactionID: transactionID,
                observedTokens: observedTokens.map(tokenString).sorted(),
                message: "completed transaction after reconciliation",
                metadata: ["observedTXIDs": observedTokens.map(tokenString).sorted().joined(separator: ",")]
            )
        }

        await transitionLifecycle(to: .ready, reason: "adapter batch applied", errorMessage: nil, lastSyncedAt: lastSyncedAt)
        await drainDispatchIfNeeded()
    }

    func didRefreshComplete(lastSyncedAt: Date?) async {
        let transactionIDs = awaitingRefreshTransactionIDs
        awaitingRefreshTransactionIDs.removeAll()

        for transactionID in transactionIDs {
            writeGate.withCriticalSection {
                reconciler.resolveTransaction(id: transactionID, collectionID: collectionID)
            }
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
        await transitionLifecycle(to: .ready, reason: "adapter refresh completed", errorMessage: nil, lastSyncedAt: lastSyncedAt)
    }

    func didEncounterAdapterError(_ error: Error) async {
        await transitionLifecycle(
            to: .error(String(describing: error)),
            reason: "adapter error",
            errorMessage: String(describing: error)
        )
        debug("adapter error for \(configuration.debugName): \(error)")
    }

    func recordedDebugEvents() -> [String] {
        debugEvents
    }

    private func enqueueDispatch(ids: [UUID]) async {
        appendPendingDispatch(ids: ids)
        await drainDispatchIfNeeded()
    }

    /*
     * A write returns once its transaction is durably queued. Reaching the
     * server is the outbox's job, not the caller's: draining inline made every
     * local write wait for a round trip, which on a slow network turns an
     * offline-capable write into a stall.
     *
     * Enqueueing stays synchronous so `pendingDispatchIDs` keeps call order;
     * only the drain is handed to a task. A drain already in progress picks up
     * the appended ids on its next loop, and the re-entrancy guard makes the
     * extra task a no-op.
     *
     * Callers that genuinely need the round trip await
     * `CollectionTransaction.wait()`; tests force a drain with `flush()`.
     */
    private func enqueueDispatchWithoutWaiting(ids: [UUID]) {
        appendPendingDispatch(ids: ids)
        Task { await self.drainDispatchIfNeeded() }
    }

    private func appendPendingDispatch(ids: [UUID]) {
        if ids.isEmpty == false {
            cancelScheduledRetry()
        }
        for id in ids where pendingDispatchIDs.contains(id) == false {
            pendingDispatchIDs.append(id)
        }
    }

    private func drainDispatchIfNeeded() async {
        guard isDrainingDispatch == false else { return }
        guard connectivityState == .online else {
            cancelScheduledRetry()
            trace(
                .dispatchPausedOffline,
                message: "dispatch paused while offline",
                metadata: ["connectivity": connectivityState.rawValue]
            )
            await transitionLifecycle(to: .offline, reason: "dispatch paused while offline", errorMessage: nil)
            return
        }
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
            await transitionLifecycle(to: .replaying, reason: "eligible transactions scheduled for replay", errorMessage: nil)
            trace(
                .replayStarted,
                message: "starting replay drain for eligible transactions",
                metadata: ["transactionIDs": eligibleIDs.map(\.uuidString).joined(separator: ",")]
            )
            pendingDispatchIDs.append(contentsOf: eligibleIDs)
        }
    }

    private func processPendingTransaction(id: UUID) async {
        guard connectivityState == .online else { return }
        let transactionRecord = queue.fetchPendingTransaction(id: id, collectionID: collectionID)
        guard let transactionRecord else { return }
        guard transactionRecord.status == .pending || transactionRecord.status == .failed else { return }
        guard transactionRecord.nextRetryAt.map({ $0 <= Date() }) ?? true else { return }
        guard queue.hasUnresolvedPredecessor(for: transactionRecord, collectionID: collectionID) == false else {
            trace(
                .dispatchEnqueued,
                transactionID: id,
                sequenceNumber: transactionRecord.sequenceNumber,
                attemptCount: transactionRecord.attemptCount,
                message: "deferred dispatch behind earlier same-key transaction",
                metadata: transactionTraceMetadata(transactionRecord)
            )
            return
        }

        let pendingMutations = queue.fetchPendingMutations(transactionID: id)
        guard pendingMutations.isEmpty == false else { return }
        let compactedDispatch = compactPendingSuccessors(
            for: transactionRecord,
            pendingMutations: pendingMutations
        )
        let representedTransactions = [transactionRecord] + compactedDispatch.transactions
        let representedTransactionIDs = representedTransactions.map(\.id)
        let stateMutations = pendingMutations + compactedDispatch.mutations
        let touchedKeys = Set(stateMutations.map(\.targetKey))

        for transaction in representedTransactions {
            transaction.status = .sending
            transaction.recordAttempt()
            transaction.lastErrorMessage = nil
            transaction.nextRetryAt = nil
        }
        for mutation in stateMutations {
            mutation.status = .sending
            mutation.recordAttempt()
            mutation.errorMessage = nil
            mutation.nextRetryAt = nil
        }
        try? saveQueueContext()
        refreshPendingModelStates(keys: touchedKeys)

        let transaction = liveTransactions[id] ?? CollectionTransaction(id: id, collectionID: collectionID)
        liveTransactions[id] = transaction
        for transactionID in representedTransactionIDs {
            let liveTransaction = liveTransactions[transactionID] ?? CollectionTransaction(id: transactionID, collectionID: collectionID)
            liveTransactions[transactionID] = liveTransaction
            await liveTransaction.markSending()
        }
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
                for representedTransaction in representedTransactions {
                    representedTransaction.setCompletion(immediateCompletion)
                    writeGate.withCriticalSection {
                        reconciler.resolveTransaction(
                            id: representedTransaction.id,
                            collectionID: collectionID
                        )
                    }
                }
                try saveQueueContext()
                refreshPendingModelStates(keys: touchedKeys)
                for representedTransaction in representedTransactions {
                    if let liveTransaction = liveTransactions.removeValue(forKey: representedTransaction.id) {
                        await liveTransaction.complete()
                    }
                    trace(
                        .transactionCompleted,
                        transactionID: representedTransaction.id,
                        sequenceNumber: representedTransaction.sequenceNumber,
                        attemptCount: representedTransaction.attemptCount,
                        message: representedTransaction.id == id
                            ? "completed transaction immediately"
                            : "completed compacted transaction immediately"
                    )
                }

            case .awaitTokens(let tokens):
                guard tokens.isEmpty == false else {
                    throw CollectionError.missingAwaitedObservationTokens
                }

                for representedTransaction in representedTransactions {
                    representedTransaction.setCompletion(.awaitTokens(tokens))
                    representedTransaction.status = .awaitingSync
                    representedTransaction.lastErrorMessage = nil
                }
                for mutation in stateMutations {
                    mutation.status = .awaitingSync
                    mutation.errorMessage = nil
                }
                try saveQueueContext()
                refreshPendingModelStates(keys: touchedKeys)

                for representedTransaction in representedTransactions {
                    register(transactionID: representedTransaction.id, awaiting: tokens)
                    if let liveTransaction = liveTransactions[representedTransaction.id] {
                        await liveTransaction.markAwaitingSync()
                    }
                    trace(
                        .awaitingSync,
                        transactionID: representedTransaction.id,
                        sequenceNumber: representedTransaction.sequenceNumber,
                        attemptCount: representedTransaction.attemptCount,
                        awaitedTokens: tokens.map(tokenString).sorted(),
                        pendingMutationCount: representedTransaction.id == id
                            ? pendingMutations.count
                            : queue.fetchPendingMutations(transactionID: representedTransaction.id).count,
                        message: representedTransaction.id == id
                            ? "awaiting observation tokens from adapter"
                            : "awaiting observation tokens from compacted adapter dispatch"
                    )
                }

            case .refresh:
                let refreshCompletion: CollectionMutationCompletion = .refresh
                for representedTransaction in representedTransactions {
                    representedTransaction.setCompletion(refreshCompletion)
                    representedTransaction.status = .awaitingSync
                    representedTransaction.lastErrorMessage = nil
                }
                for mutation in stateMutations {
                    mutation.status = .awaitingSync
                    mutation.errorMessage = nil
                }
                try saveQueueContext()
                refreshPendingModelStates(keys: touchedKeys)
                for representedTransaction in representedTransactions {
                    awaitingRefreshTransactionIDs.insert(representedTransaction.id)
                    if let liveTransaction = liveTransactions[representedTransaction.id] {
                        await liveTransaction.markAwaitingSync()
                    }
                    trace(
                        .awaitingSync,
                        transactionID: representedTransaction.id,
                        sequenceNumber: representedTransaction.sequenceNumber,
                        attemptCount: representedTransaction.attemptCount,
                        pendingMutationCount: representedTransaction.id == id
                            ? pendingMutations.count
                            : queue.fetchPendingMutations(transactionID: representedTransaction.id).count,
                        message: representedTransaction.id == id
                            ? "awaiting adapter refresh completion"
                            : "awaiting adapter refresh completion from compacted dispatch"
                    )
                }
                await adapterRuntime.refresh()
            }
        } catch {
            if isNonRetriable(error) {
                for representedTransaction in representedTransactions {
                    representedTransaction.status = .conflicted
                    representedTransaction.lastErrorMessage = String(describing: error)
                    representedTransaction.nextRetryAt = nil
                }
                for mutation in stateMutations {
                    mutation.status = .conflicted
                    mutation.errorMessage = String(describing: error)
                    mutation.nextRetryAt = nil
                }
                try? saveQueueContext()
            } else {
                for representedTransaction in representedTransactions {
                    representedTransaction.markFailed(error, retryPolicy: retryPolicy)
                }
                for mutation in stateMutations {
                    mutation.markFailed(error, retryPolicy: retryPolicy)
                }
                try? saveQueueContext()
            }

            refreshPendingModelStates(keys: touchedKeys)

            for representedTransactionID in representedTransactionIDs {
                if let liveTransaction = liveTransactions.removeValue(forKey: representedTransactionID) {
                    await liveTransaction.fail(error)
                }
            }

            await transitionLifecycle(
                to: .error(String(describing: error)),
                reason: "dispatch failed",
                errorMessage: String(describing: error)
            )
            trace(
                .transactionFailed,
                transactionID: id,
                sequenceNumber: transactionRecord.sequenceNumber,
                attemptCount: transactionRecord.attemptCount,
                pendingMutationCount: pendingMutations.count,
                message: isNonRetriable(error) ? "dispatch failed permanently" : "dispatch failed",
                error: error,
                metadata: ["nonRetriable": String(isNonRetriable(error))]
            )
            debug("failed dispatch for \(configuration.debugName) transaction \(id): \(error)")
        }
    }

    private func scheduleNextRetryIfNeeded(now: Date = Date()) {
        guard connectivityState == .online else {
            cancelScheduledRetry()
            return
        }
        let nextRetryAt = queue.nextRetryAt(collectionID: collectionID, now: now)
        guard let nextRetryAt else {
            cancelScheduledRetry()
            return
        }
        guard scheduledRetryAt != nextRetryAt else { return }

        cancelScheduledRetry()
        scheduledRetryAt = nextRetryAt
        let delay = max(0, nextRetryAt.timeIntervalSince(now))
        trace(
            .retryScheduled,
            message: "scheduled next failed transaction retry",
            metadata: [
                "nextRetryAt": isoString(nextRetryAt),
                "delay": String(delay),
            ]
        )
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
        trace(
            .retryFired,
            message: "scheduled retry fired",
            metadata: ["expectedRetryAt": isoString(expectedRetryAt)]
        )
        await drainDispatchIfNeeded()
    }

    private func cancelScheduledRetry() {
        scheduledRetryTask?.cancel()
        scheduledRetryTask = nil
        scheduledRetryAt = nil
    }

    private func makeFailedTransactionsEligibleForReconnectRetry(now: Date = Date()) {
        let failedTransactions = queue.fetchAllPendingTransactions(collectionID: collectionID)
            .filter { $0.status == .failed }
        guard failedTransactions.isEmpty == false else { return }

        for transaction in failedTransactions {
            transaction.nextRetryAt = now
            transaction.updatedAt = now
            for mutation in queue.fetchPendingMutations(transactionID: transaction.id) where mutation.status == .failed {
                mutation.nextRetryAt = now
            }
        }
        try? saveQueueContext()
    }

    private func commitPreparedTransaction(
        _ preparedTransaction: PreparedCollectionTransaction,
        pendingTransaction: PendingCollectionTransaction,
        persistedMutations: [PendingCollectionMutation]
    ) throws {
        try writeGate.withCriticalSection {
            let context = ModelContext(modelContainer)
            try validatePromotions(
                preparedTransaction.optimisticChanges,
                in: context
            )
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
    }

    private struct CompactedDispatch {
        let transactions: [PendingCollectionTransaction]
        let mutations: [PendingCollectionMutation]
    }

    private func compactPendingSuccessors(
        for transaction: PendingCollectionTransaction,
        pendingMutations: [PendingCollectionMutation]
    ) -> CompactedDispatch {
        guard pendingMutations.count == 1,
              let baseMutation = pendingMutations.first,
              baseMutation.operation == .create || baseMutation.operation == .update else {
            return CompactedDispatch(transactions: [], mutations: [])
        }

        let successors = queue.compactableSuccessorTransactions(
            after: transaction,
            collectionID: collectionID,
            targetKey: baseMutation.targetKey,
            operation: .update,
            now: Date()
        )
        guard successors.isEmpty == false else {
            return CompactedDispatch(transactions: [], mutations: [])
        }

        var latestPayload = baseMutation.payload
        var latestMetadata = baseMutation.metadata
        var compactedTransactions: [PendingCollectionTransaction] = []
        var compactedMutations: [PendingCollectionMutation] = []

        for successor in successors {
            latestPayload = successor.mutation.payload
            if successor.mutation.metadata.isEmpty == false {
                latestMetadata = successor.mutation.metadata
            }
            compactedTransactions.append(successor.transaction)
            compactedMutations.append(successor.mutation)
        }

        baseMutation.payload = latestPayload
        baseMutation.metadata = latestMetadata
        switch baseMutation.operation {
        case .create:
            baseMutation.changedFields = Set(latestPayload.keys)
        case .update:
            baseMutation.changedFields = changedFields(
                from: baseMutation.originalRow ?? [:],
                to: latestPayload
            )
        case .delete:
            break
        }

        trace(
            .mutationMerged,
            transactionID: transaction.id,
            key: baseMutation.targetKey,
            operation: baseMutation.operation,
            pendingMutationCount: compactedMutations.count + 1,
            message: "compacted pending same-key updates into outbound \(baseMutation.operation.rawValue)",
            metadata: [
                "compactedTransactionIDs": compactedTransactions.map(\.id.uuidString).joined(separator: ","),
                "compactedMutationCount": String(compactedMutations.count),
                "changedFields": baseMutation.changedFields.sorted().joined(separator: ","),
            ]
        )

        return CompactedDispatch(
            transactions: compactedTransactions,
            mutations: compactedMutations
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
                message: "invoking outbound mutation handler",
                metadata: [
                    "keys": group.map(\.targetKey).joined(separator: ","),
                    "mutations": debugString(mutations.map(mutationDebugPayload)),
                ]
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
                    message: "outbound handler completed immediately",
                    metadata: ["completion": "immediate"]
                )
            case .refresh:
                requiresRefresh = true
                trace(
                    .handlerReturned,
                    transactionID: transaction.id,
                    key: group.first?.targetKey,
                    operation: group.first?.operation,
                    pendingMutationCount: group.count,
                    message: "outbound handler requested refresh completion",
                    metadata: ["completion": "refresh"]
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
                    message: "outbound handler returned awaited observation tokens",
                    metadata: [
                        "completion": "awaitTokens",
                        "awaitedTXIDs": tokens.map(tokenString).sorted().joined(separator: ","),
                    ]
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
        for change in changes {
            switch change {
            case .create(_, let row):
                let model = try Model(collectionRow: row, decoder: rowDecoder)
                context.insert(model)
            case .promoteExistingCreate(let key, _):
                guard let model = try fetchModel(key: key, in: context) else {
                    throw CollectionError.modelNotFound(key)
                }
                model.collectionSyncState = .stagedCreate
                model.collectionPendingMutationCount = 0
            case .update(let key, let row):
                guard let model = try fetchModel(key: key, in: context) else {
                    throw CollectionError.modelNotFound(key)
                }
                try model.apply(collectionRow: row, decoder: rowDecoder)
            case .delete(let key):
                guard let model = try fetchModel(key: key, in: context) else {
                    throw CollectionError.modelNotFound(key)
                }
                model.collectionSyncState = .pendingDelete
            }
        }
    }

    private func validatePromotions(
        _ changes: [OptimisticModelChange],
        in context: ModelContext
    ) throws {
        for change in changes {
            guard case .promoteExistingCreate(let key, _) = change else { continue }
            let pending = CollectionMutationReconciler.unresolvedMutations(
                modelName: configuration.modelName,
                targetKey: key,
                in: context
            )
            guard pending.isEmpty else {
                throw CollectionError.stagedOperationHasPendingMutations(key: key, count: pending.count)
            }
            guard let model = try fetchModel(key: key, in: context) else {
                throw CollectionError.modelNotFound(key)
            }
            guard model.collectionSyncState == .stagedCreate else {
                throw CollectionError.invalidStagedTransition(key: key, state: model.collectionSyncState)
            }
            let actualKey = configuration.identifier.key(for: model)
            guard actualKey == key else {
                throw CollectionError.stableIdentifierChanged(expected: key, actual: actualKey)
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
                message: message,
                metadata: mutationTraceMetadata(mutation)
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
        trace(
            .awaitedTokensRegistered,
            transactionID: transactionID,
            awaitedTokens: tokens.map(tokenString).sorted(),
            message: "registered awaited observation tokens",
            metadata: ["awaitedTXIDs": tokens.map(tokenString).sorted().joined(separator: ",")]
        )
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

        writeGate.withCriticalSection {
            let context = ModelContext(modelContainer)
            for key in keysToRefresh {
                try? CollectionMutationReconciler.refreshModelState(
                    for: Model.self,
                    key: key,
                    modelName: configuration.modelName,
                    identifier: configuration.identifier,
                    in: context
                )
            }
            try? commitSave(context)
        }
        trace(
            .pendingStateRefreshed,
            pendingMutationCount: queue.fetchAllPendingMutations(collectionID: collectionID).count,
            message: "refreshed pending row sync state",
            metadata: ["keys": keysToRefresh.sorted().joined(separator: ",")]
        )
    }

    private func transitionLifecycle(
        to newState: CollectionLifecycleState,
        reason: String,
        errorMessage: String?,
        lastSyncedAt: Date? = nil
    ) async {
        let oldState = lifecycleState
        lifecycleState = newState
        trace(
            .lifecycleChanged,
            message: reason,
            metadata: [
                "from": lifecycleDebugString(oldState),
                "to": lifecycleDebugString(newState),
            ]
        )
        await persistLifecycleState(errorMessage: errorMessage, lastSyncedAt: lastSyncedAt)
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
        try? saveQueueContext()
    }

    private func debug(_ message: String) {
        debugEvents.append(message)
        debugLogger.log(.debug, category: "CollectionCoordinator", message: message)
    }

    private func saveQueueContext() throws {
        try writeGate.withCriticalSection {
            try queue.saveContext()
        }
    }

    private func tokenString(_ token: String) -> String {
        String(describing: token)
    }

    private func transactionTraceMetadata(_ transaction: PendingCollectionTransaction) -> [String: String] {
        [
            "status": transaction.status.rawValue,
            "completion": transaction.completion().map(String.init(describing:)) ?? "",
            "awaitedTokens": transaction.awaitedObservationTokens.joined(separator: ","),
            "awaitedTXIDs": transaction.awaitedObservationTokens.joined(separator: ","),
            "nextRetryAt": transaction.nextRetryAt.map(isoString) ?? "",
            "mutationCount": String(queue.fetchPendingMutations(transactionID: transaction.id).count),
        ]
    }

    private func transactionDebugSummary(_ transaction: PendingCollectionTransaction) -> String {
        [
            "id=\(transaction.id.uuidString)",
            "seq=\(transaction.sequenceNumber)",
            "status=\(transaction.status.rawValue)",
            "attempt=\(transaction.attemptCount)",
            "awaited=\(transaction.awaitedObservationTokens.joined(separator: ","))",
        ].joined(separator: " ")
    }

    private func mutationTraceMetadata(_ mutation: CollectionMutation) -> [String: String] {
        var metadata: [String: String] = [
            "changes": debugString(mutation.changes),
            "changedFields": mutation.changes.keys.sorted().joined(separator: ","),
            "metadata": debugString(mutation.metadata),
        ]
        if let original = mutation.original {
            metadata["original"] = debugString(original)
        }
        if let modified = mutation.modified {
            metadata["modified"] = debugString(modified)
        }
        return metadata
    }

    private func mutationDebugPayload(_ mutation: CollectionMutation) -> MutationDebugPayload {
        MutationDebugPayload(
            operation: mutation.operation.rawValue,
            key: mutation.key,
            original: mutation.original,
            modified: mutation.modified,
            changes: mutation.changes,
            metadata: mutation.metadata.mapValues(String.init(describing:))
        )
    }

    private struct MutationDebugPayload: Encodable {
        let operation: String
        let key: String
        let original: CollectionRow?
        let modified: CollectionRow?
        let changes: CollectionRow
        let metadata: [String: String]
    }

    private func debugString<T: Encodable>(_ value: T) -> String {
        guard let data = try? JSONEncoder().encode(value),
              let string = String(data: data, encoding: .utf8) else {
            return String(describing: value)
        }
        return string
    }

    private func changedFields(from original: CollectionRow, to modified: CollectionRow) -> Set<String> {
        Set(modified.compactMap { key, value in
            original[key] == value ? nil : key
        })
    }

    private func lifecycleDebugString(_ state: CollectionLifecycleState) -> String {
        switch state {
        case .idle: "idle"
        case .bootstrapping: "bootstrapping"
        case .syncing: "syncing"
        case .replaying: "replaying"
        case .ready: "ready"
        case .offline: "offline"
        case .error(let message): "error(\(message))"
        }
    }

    private func isoString(_ date: Date) -> String {
        ISO8601DateFormatter().string(from: date)
    }

    private func isNonRetriable(_ error: Error) -> Bool {
        error is CollectionNonRetriableError
    }

    private func trace(
        _ kind: CollectionTraceEventKind,
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
        tracer.record(
            CollectionTraceEvent(
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
