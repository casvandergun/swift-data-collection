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
    private var conflictContinuations: [
        UUID: AsyncThrowingStream<[CollectionConflict], any Error>.Continuation
    ] = [:]

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
            if transaction.status == .awaiting,
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
                    // Immediate completion is already represented by resolved
                    // mutation records. Keep bootstrap's cached context as the
                    // sole writer here; resolving through a second context and
                    // then saving this stale queue context could resurrect the
                    // awaiting state after a restart.
                    transaction.status = .resolved
                    for mutation in queue.fetchPendingMutations(transactionID: transaction.id) {
                        mutation.status = .resolved
                        mutation.errorMessage = nil
                    }
                }
            }
        }

        try? saveQueueContext()
        invalidateQueueContext()
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

    func conflicts() async throws -> [CollectionConflict] {
        await bootstrapIfNeeded()
        return try conflictSnapshot()
    }

    func conflictUpdates() async -> AsyncThrowingStream<[CollectionConflict], any Error> {
        await bootstrapIfNeeded()
        let subscriptionID = UUID()
        let pair = AsyncThrowingStream<[CollectionConflict], any Error>.makeStream(
            bufferingPolicy: .bufferingNewest(1)
        )
        conflictContinuations[subscriptionID] = pair.continuation
        pair.continuation.onTermination = { [weak self] _ in
            Task { await self?.removeConflictContinuation(subscriptionID) }
        }
        do {
            pair.continuation.yield(try conflictSnapshot())
        } catch {
            conflictContinuations.removeValue(forKey: subscriptionID)
            pair.continuation.finish(throwing: error)
        }
        return pair.stream
    }

    func discard(_ conflictID: UUID) async throws {
        await bootstrapIfNeeded()
        do {
            try writeGate.withCriticalSection {
                let context = ModelContext(modelContainer)
                let allTransactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
                    .filter { $0.collectionID == collectionID }
                let members = allTransactions
                    .filter { ($0.dispatchGroupID ?? $0.id) == conflictID }
                    .sorted(by: Self.transactionIsEarlier)

                guard members.isEmpty == false else {
                    throw CollectionConflictError.notFound(conflictID)
                }
                if members.allSatisfy({ $0.status == .discarded }) {
                    return
                }
                guard members.allSatisfy({ $0.status == .conflicted }) else {
                    throw CollectionConflictError.notConflicted(conflictID)
                }

                guard let leader = members.first(where: { $0.id == conflictID }),
                      leader.submittedMutationsData != nil else {
                    throw CollectionConflictError.unsupportedLegacySubmission(conflictID)
                }

                let memberIDs = Set(members.map(\.id))
                let mutations = try context.fetch(FetchDescriptor<PendingCollectionMutation>())
                    .filter { memberIDs.contains($0.transactionID) }
                let keys = Set(mutations.map(\.targetKey))
                let materializer = makeMaterializer(in: context)
                let unknownKeys = try keys.filter {
                    guard let evidence = try materializer.baselineEvidence(for: $0) else { return true }
                    return evidence == .unknown
                }.sorted()
                guard unknownKeys.isEmpty else {
                    throw CollectionConflictError.requiresAuthoritativeRecovery(
                        conflictID,
                        keys: unknownKeys
                    )
                }

                for transaction in members {
                    transaction.status = .discarded
                    transaction.nextRetryAt = nil
                }
                for mutation in mutations {
                    mutation.status = .discarded
                    mutation.nextRetryAt = nil
                }
                try materializer.rebuildNeverSubmittedSuccessorPayloads(for: keys)
                try materializer.materialize(keys: keys)
                try commitSave(context)
            }
        } catch {
            // The fresh write context is discarded, but the long-lived queue
            // context may have retained objects from before the failed commit.
            // Clear it before the next eligibility scan, otherwise a failed
            // discard can make a successor appear permanently blocked.
            invalidateQueueContext()
            throw error
        }
        invalidateQueueContext()

        publishConflictSnapshot()
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

            let persistedMutations = try preparedTransaction.persistedMutations()
            let sequenceNumber = try commitPreparedTransaction(
                preparedTransaction,
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

        let completedTransactionIDs: [UUID]
        do {
            completedTransactionIDs = try writeGate.withCriticalSection {
                try reconciler.resolveTransactions(
                    observedTokens: observedTokens,
                    collectionID: collectionID,
                    remainingTokensByTransactionID: &remainingTokensByTransactionID,
                    awaitedTransactionIDsByToken: &awaitedTransactionIDsByToken,
                    materialize: { context, keys in
                        try self.makeMaterializer(in: context).materialize(keys: keys)
                    }
                )
            }
            invalidateQueueContext()
        } catch {
            invalidateQueueContext()
            await transitionLifecycle(
                to: .error(String(describing: error)),
                reason: "failed to persist adapter acknowledgement",
                errorMessage: String(describing: error)
            )
            return
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
        publishConflictSnapshot()
        await drainDispatchIfNeeded()
    }

    func didRefreshComplete(lastSyncedAt: Date?) async {
        let transactionIDs = awaitingRefreshTransactionIDs
        awaitingRefreshTransactionIDs.removeAll()

        for transactionID in transactionIDs {
            let resolved = (try? writeGate.withCriticalSection {
                try reconciler.resolveTransaction(
                    id: transactionID,
                    collectionID: collectionID,
                    materialize: { context, keys in
                        try self.makeMaterializer(in: context).materialize(keys: keys)
                    }
                )
            }) ?? false
            guard resolved else {
                awaitingRefreshTransactionIDs.insert(transactionID)
                continue
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
        invalidateQueueContext()
        publishConflictSnapshot()
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
                guard await processPendingTransaction(id: id) else { return }
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

    private func processPendingTransaction(id: UUID) async -> Bool {
        guard connectivityState == .online else { return true }
        let transactionRecord = queue.fetchPendingTransaction(id: id, collectionID: collectionID)
        guard let transactionRecord else { return true }
        guard transactionRecord.status == .pending || transactionRecord.status == .failed else { return true }
        guard transactionRecord.nextRetryAt.map({ $0 <= Date() }) ?? true else { return true }
        guard queue.hasUnresolvedPredecessor(for: transactionRecord, collectionID: collectionID) == false else {
            trace(
                .dispatchEnqueued,
                transactionID: id,
                sequenceNumber: transactionRecord.sequenceNumber,
                attemptCount: transactionRecord.attemptCount,
                message: "deferred dispatch behind earlier same-key transaction",
                metadata: transactionTraceMetadata(transactionRecord)
            )
            return true
        }

        let dispatch: PreparedDispatchGroup
        do {
            dispatch = try prepareDispatchGroup(for: id)
        } catch {
            invalidateQueueContext()
            if let liveTransaction = liveTransactions.removeValue(forKey: id) {
                await liveTransaction.fail(error)
            }
            await transitionLifecycle(
                to: .error(String(describing: error)),
                reason: "failed to persist submitted dispatch representation",
                errorMessage: String(describing: error)
            )
            return false
        }

        let transaction = liveTransactions[dispatch.id]
            ?? CollectionTransaction(id: dispatch.id, collectionID: collectionID)
        liveTransactions[dispatch.id] = transaction
        for transactionID in dispatch.transactionIDs {
            let liveTransaction = liveTransactions[transactionID] ?? CollectionTransaction(id: transactionID, collectionID: collectionID)
            liveTransactions[transactionID] = liveTransaction
            await liveTransaction.markSending()
        }
        trace(
            .dispatchStarted,
            transactionID: dispatch.id,
            sequenceNumber: dispatch.sequenceNumber,
            attemptCount: dispatch.attemptCount,
            pendingMutationCount: dispatch.mutations.count,
            message: "dispatching queued transaction"
        )

        do {
            let completion = try await dispatchMutationGroups(
                transaction: transaction,
                mutations: dispatch.mutations
            )

            switch completion {
            case .immediate:
                try completeImmediately(dispatch)
                for representedTransactionID in dispatch.transactionIDs {
                    if let liveTransaction = liveTransactions.removeValue(forKey: representedTransactionID) {
                        await liveTransaction.complete()
                    }
                    trace(
                        .transactionCompleted,
                        transactionID: representedTransactionID,
                        sequenceNumber: dispatch.sequenceNumber,
                        attemptCount: dispatch.attemptCount,
                        message: representedTransactionID == dispatch.id
                            ? "completed transaction immediately"
                            : "completed compacted transaction immediately"
                    )
                }

            case .awaitTokens(let tokens):
                guard tokens.isEmpty == false else {
                    throw CollectionError.missingAwaitedObservationTokens
                }

                try markDispatchGroupAwaiting(dispatch, completion: .awaitTokens(tokens))

                for representedTransactionID in dispatch.transactionIDs {
                    register(transactionID: representedTransactionID, awaiting: tokens)
                    if let liveTransaction = liveTransactions[representedTransactionID] {
                        await liveTransaction.markAwaiting()
                    }
                    trace(
                        .awaiting,
                        transactionID: representedTransactionID,
                        sequenceNumber: dispatch.sequenceNumber,
                        attemptCount: dispatch.attemptCount,
                        awaitedTokens: tokens.map(tokenString).sorted(),
                        pendingMutationCount: queue.fetchPendingMutations(transactionID: representedTransactionID).count,
                        message: representedTransactionID == dispatch.id
                            ? "awaiting observation tokens from adapter"
                            : "awaiting observation tokens from compacted adapter dispatch"
                    )
                }

            case .refresh:
                try markDispatchGroupAwaiting(dispatch, completion: .refresh)
                for representedTransactionID in dispatch.transactionIDs {
                    awaitingRefreshTransactionIDs.insert(representedTransactionID)
                    if let liveTransaction = liveTransactions[representedTransactionID] {
                        await liveTransaction.markAwaiting()
                    }
                    trace(
                        .awaiting,
                        transactionID: representedTransactionID,
                        sequenceNumber: dispatch.sequenceNumber,
                        attemptCount: dispatch.attemptCount,
                        pendingMutationCount: queue.fetchPendingMutations(transactionID: representedTransactionID).count,
                        message: representedTransactionID == dispatch.id
                            ? "awaiting adapter refresh completion"
                            : "awaiting adapter refresh completion from compacted dispatch"
                    )
                }
                await adapterRuntime.refresh()
            }
        } catch {
            do {
                try markDispatchGroupFailed(dispatch, error: error)
            } catch {
                // The group remains in its last durable state (normally
                // `sending`). Stop this drain: continuing with a stale queue
                // context can spin forever and/or invoke a handler twice.
                invalidateQueueContext()
                debug("failed to persist dispatch failure for \(configuration.debugName): \(error)")
                for representedTransactionID in dispatch.transactionIDs {
                    if let liveTransaction = liveTransactions.removeValue(forKey: representedTransactionID) {
                        await liveTransaction.fail(error)
                    }
                }
                await transitionLifecycle(
                    to: .error(String(describing: error)),
                    reason: "failed to persist dispatch failure",
                    errorMessage: String(describing: error)
                )
                return false
            }

            for representedTransactionID in dispatch.transactionIDs {
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
                transactionID: dispatch.id,
                sequenceNumber: dispatch.sequenceNumber,
                attemptCount: dispatch.attemptCount,
                pendingMutationCount: dispatch.mutations.count,
                message: isNonRetriable(error) ? "dispatch failed permanently" : "dispatch failed",
                error: error,
                metadata: ["nonRetriable": String(isNonRetriable(error))]
            )
            debug("failed dispatch for \(configuration.debugName) transaction \(dispatch.id): \(error)")
        }
        return true
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
        persistedMutations: [PendingCollectionMutation]
    ) throws -> Int {
        do {
            let sequenceNumber = try writeGate.withCriticalSection {
                let context = ModelContext(modelContainer)
                let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
                    .filter { $0.collectionID == collectionID }
                let maximumSequence = transactions.map(\.sequenceNumber).max() ?? -1
                guard maximumSequence < Int.max else {
                    throw CollectionError.transactionSequenceOverflow
                }
                let metadata = try fetchOrCreateCollectionMetadata(in: context)
                let seededNext = max(metadata.nextTransactionSequence, maximumSequence + 1)
                guard seededNext < Int.max else {
                    throw CollectionError.transactionSequenceOverflow
                }
                metadata.nextTransactionSequence = seededNext + 1

                let pendingTransaction = PendingCollectionTransaction(
                    id: preparedTransaction.transactionID,
                    collectionID: collectionID,
                    shapeID: sourceID,
                    modelName: configuration.modelName,
                    sequenceNumber: seededNext,
                    status: .pending
                )
                try validatePromotions(
                    preparedTransaction.optimisticChanges,
                    in: context
                )
                let materializer = makeMaterializer(in: context)
                for mutation in preparedTransaction.mutations {
                    try materializer.captureBaselineIfNeeded(
                        for: mutation.key,
                        operation: mutation.operation
                    )
                }
                context.insert(pendingTransaction)
                for mutation in persistedMutations {
                    context.insert(mutation)
                }

                try applyOptimisticChanges(
                    preparedTransaction.optimisticChanges,
                    in: context
                )

                for key in preparedTransaction.touchedKeys {
                    try materializer.materialize(key: key)
                }

                try commitSave(context)

                try traceCommittedOptimisticChanges(
                    preparedTransaction.mutations,
                    transactionID: preparedTransaction.transactionID,
                    in: context
                )
                return seededNext
            }
            // The queue context may have fetched this collection before the
            // fresh atomic write. Refresh it before scheduling the new ID.
            invalidateQueueContext()
            return sequenceNumber
        } catch {
            invalidateQueueContext()
            throw error
        }
    }

    private struct PreparedDispatchGroup {
        let id: UUID
        let transactionIDs: [UUID]
        let mutations: [CollectionMutation]
        let touchedKeys: Set<String>
        let sequenceNumber: Int
        let attemptCount: Int
    }

    private func prepareDispatchGroup(for requestedID: UUID) throws -> PreparedDispatchGroup {
        do {
            let dispatch = try writeGate.withCriticalSection {
                let context = ModelContext(modelContainer)
                let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
                    .filter { $0.collectionID == collectionID }
                    .sorted(by: Self.transactionIsEarlier)
                guard let requested = transactions.first(where: { $0.id == requestedID }) else {
                    throw CollectionConflictError.notFound(requestedID)
                }
                guard requested.status == .pending || requested.status == .failed else {
                    throw CollectionConflictError.notConflicted(requestedID)
                }
                let allMutations = try context.fetch(FetchDescriptor<PendingCollectionMutation>())
                let mutationsByTransaction = Dictionary(grouping: allMutations, by: \.transactionID)

                let groupID = requested.dispatchGroupID ?? requested.id
                var members: [PendingCollectionTransaction]
                var submitted: [CollectionMutation]

                if requested.dispatchGroupID != nil {
                    members = transactions.filter { $0.dispatchGroupID == groupID }
                    guard let leader = members.first(where: { $0.id == groupID }),
                          let data = leader.submittedMutationsData else {
                        throw CollectionConflictError.unsupportedLegacySubmission(groupID)
                    }
                    submitted = try JSONDecoder().decode([CollectionMutation].self, from: data)
                    guard submitted.isEmpty == false else {
                        throw CollectionError.modelNotFound(groupID.uuidString)
                    }
                } else {
                    members = [requested]
                    let leadingPending = (mutationsByTransaction[requested.id] ?? [])
                        .sorted(by: Self.mutationIsEarlier)
                    guard leadingPending.isEmpty == false else {
                        throw CollectionError.modelNotFound(requested.id.uuidString)
                    }
                    submitted = leadingPending.map(makeCollectionMutation(from:))

                    if leadingPending.count == 1,
                       let leading = leadingPending.first,
                       leading.operation == .create || leading.operation == .update {
                        for successor in transactions where Self.transactionIsEarlier(requested, successor) {
                            let pending = (mutationsByTransaction[successor.id] ?? [])
                                .sorted(by: Self.mutationIsEarlier)
                            let touchesLeadingKey = pending.contains { $0.targetKey == leading.targetKey }
                            // A transaction for another key does not affect this
                            // compaction run. Once the same key is encountered,
                            // however, an ineligible transaction is a durable
                            // ordering barrier and must not be crossed.
                            guard touchesLeadingKey else { continue }
                            guard successor.status == .pending,
                                  successor.dispatchGroupID == nil,
                                  successor.submittedMutationsData == nil,
                                  successor.attemptCount == 0 else {
                                break
                            }
                            guard pending.count == 1,
                                  let successorMutation = pending.first,
                                  successorMutation.targetKey == leading.targetKey,
                                  successorMutation.operation == .update,
                                  successorMutation.status == .pending,
                                  let merged = try CollectionMutationMerger.merge(
                                    existing: submitted[0],
                                    incoming: makeCollectionMutation(from: successorMutation)
                                  ) else {
                                break
                            }
                            submitted[0] = merged
                            members.append(successor)
                        }
                    }

                    let frozenData = try JSONEncoder().encode(submitted)
                    for member in members {
                        member.dispatchGroupID = groupID
                    }
                    requested.submittedMutationsData = frozenData
                }

                members.sort(by: Self.transactionIsEarlier)
                let memberIDs = Set(members.map(\.id))
                let stateMutations = allMutations.filter { memberIDs.contains($0.transactionID) }
                for member in members {
                    member.status = .sending
                    member.recordAttempt()
                    member.lastErrorMessage = nil
                    member.nextRetryAt = nil
                }
                for mutation in stateMutations {
                    mutation.status = .sending
                    mutation.recordAttempt()
                    mutation.errorMessage = nil
                    mutation.nextRetryAt = nil
                }
                try makeMaterializer(in: context).materialize(keys: stateMutations.map(\.targetKey))
                try commitSave(context)

                return PreparedDispatchGroup(
                    id: groupID,
                    transactionIDs: members.map(\.id),
                    mutations: submitted,
                    touchedKeys: Set(stateMutations.map(\.targetKey)),
                    sequenceNumber: members.first?.sequenceNumber ?? requested.sequenceNumber,
                    attemptCount: members.first?.attemptCount ?? requested.attemptCount
                )
            }
            invalidateQueueContext()
            return dispatch
        } catch {
            invalidateQueueContext()
            throw error
        }
    }

    private func completeImmediately(_ dispatch: PreparedDispatchGroup) throws {
        try updateDispatchGroup(dispatch) { transactions, mutations, materializer in
            for mutation in dispatch.mutations {
                switch mutation.operation {
                case .create:
                    guard let row = mutation.modified else {
                        throw CollectionMaterializationError.invalidPersistedRow(key: mutation.key)
                    }
                    try materializer.accept(.create(row), for: mutation.key)
                case .update:
                    try materializer.accept(.update(mutation.changes), for: mutation.key)
                case .delete:
                    try materializer.accept(.delete, for: mutation.key)
                }
            }
            for transaction in transactions {
                transaction.setCompletion(.immediate)
                transaction.status = .resolved
                transaction.lastErrorMessage = nil
            }
            for mutation in mutations {
                mutation.status = .resolved
                mutation.errorMessage = nil
            }
            try materializer.materialize(keys: dispatch.touchedKeys)
        }
    }

    private func markDispatchGroupAwaiting(
        _ dispatch: PreparedDispatchGroup,
        completion: CollectionMutationCompletion
    ) throws {
        try updateDispatchGroup(dispatch) { transactions, mutations, materializer in
            for transaction in transactions {
                transaction.setCompletion(completion)
                transaction.status = .awaiting
                transaction.lastErrorMessage = nil
            }
            for mutation in mutations {
                mutation.status = .awaiting
                mutation.errorMessage = nil
            }
            try materializer.materialize(keys: dispatch.touchedKeys)
        }
    }

    private func markDispatchGroupFailed(
        _ dispatch: PreparedDispatchGroup,
        error: Error
    ) throws {
        let nonRetriable = isNonRetriable(error)
        try updateDispatchGroup(dispatch) { transactions, mutations, materializer in
            let now = Date()
            for transaction in transactions {
                if nonRetriable {
                    transaction.status = .conflicted
                    transaction.lastErrorMessage = String(describing: error)
                    transaction.nextRetryAt = nil
                    transaction.conflictOccurredAt = now
                } else {
                    transaction.markFailed(error, retryPolicy: retryPolicy, now: now)
                }
            }
            for mutation in mutations {
                if nonRetriable {
                    mutation.status = .conflicted
                    mutation.errorMessage = String(describing: error)
                    mutation.nextRetryAt = nil
                } else {
                    mutation.markFailed(error, retryPolicy: retryPolicy, now: now)
                }
            }
            try materializer.materialize(keys: dispatch.touchedKeys)
        }
        if nonRetriable {
            publishConflictSnapshot()
        }
    }

    private func updateDispatchGroup(
        _ dispatch: PreparedDispatchGroup,
        _ update: (
            [PendingCollectionTransaction],
            [PendingCollectionMutation],
            CollectionMaterializer<Model, ID>
        ) throws -> Void
    ) throws {
        do {
            try writeGate.withCriticalSection {
                let context = ModelContext(modelContainer)
                let memberIDs = Set(dispatch.transactionIDs)
                let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
                    .filter { $0.collectionID == collectionID && memberIDs.contains($0.id) }
                let mutations = try context.fetch(FetchDescriptor<PendingCollectionMutation>())
                    .filter { memberIDs.contains($0.transactionID) }
                try update(transactions, mutations, makeMaterializer(in: context))
                try commitSave(context)
            }
            invalidateQueueContext()
        } catch {
            invalidateQueueContext()
            throw error
        }
    }

    private func dispatchMutationGroups(
        transaction: CollectionTransaction,
        mutations: [CollectionMutation]
    ) async throws -> CollectionMutationCompletion {
        var awaitedTokens = Set<String>()
        var requiresRefresh = false

        for group in CollectionMutationDispatcher.groups(from: mutations) {
            let context = CollectionMutationContext<Model, ID>(transaction: transaction, mutations: group)
            trace(
                .handlerInvoked,
                transactionID: transaction.id,
                key: group.first?.key,
                operation: group.first?.operation,
                pendingMutationCount: group.count,
                message: "invoking outbound mutation handler",
                metadata: [
                    "keys": group.map(\.key).joined(separator: ","),
                    "mutations": debugString(group.map(mutationDebugPayload)),
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
                    key: group.first?.key,
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
                    key: group.first?.key,
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
                    key: group.first?.key,
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

    private func fetchOrCreateCollectionMetadata(in context: ModelContext) throws -> CollectionMetadata {
        if let existing = try context.fetch(FetchDescriptor<CollectionMetadata>())
            .first(where: { $0.collectionID == collectionID }) {
            return existing
        }
        let metadata = CollectionMetadata(
            collectionID: collectionID,
            shapeID: sourceID,
            modelName: configuration.modelName,
            debugName: configuration.debugName
        )
        context.insert(metadata)
        return metadata
    }

    private func makeMaterializer(
        in context: ModelContext
    ) -> CollectionMaterializer<Model, ID> {
        CollectionMaterializer(
            context: context,
            collectionID: collectionID,
            modelName: configuration.modelName,
            identifier: configuration.identifier,
            rowDecoder: rowDecoder
        )
    }

    private func conflictSnapshot() throws -> [CollectionConflict] {
        try writeGate.withCriticalSection {
            let context = ModelContext(modelContainer)
            let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
                .filter { $0.collectionID == collectionID }
                .sorted(by: Self.transactionIsEarlier)
            let conflictedGroupIDs = Set(
                transactions
                    .filter { $0.status == .conflicted }
                    .map { $0.dispatchGroupID ?? $0.id }
            )
            let allMutations = try context.fetch(FetchDescriptor<PendingCollectionMutation>())
            let materializer = makeMaterializer(in: context)

            return try conflictedGroupIDs.map { groupID in
                let members = transactions
                    .filter { ($0.dispatchGroupID ?? $0.id) == groupID }
                    .sorted(by: Self.transactionIsEarlier)
                let memberIDs = Set(members.map(\.id))
                let sourceMutations = allMutations
                    .filter { memberIDs.contains($0.transactionID) }
                    .sorted(by: Self.mutationIsEarlier)
                let leader = members.first(where: { $0.id == groupID }) ?? members[0]
                let submittedMutations: [CollectionMutation]?
                if let data = leader.submittedMutationsData {
                    submittedMutations = try JSONDecoder().decode([CollectionMutation].self, from: data)
                } else {
                    submittedMutations = nil
                }
                let representedMutations = submittedMutations ?? sourceMutations.map(makeCollectionMutation(from:))

                var containsUnknownBaseline = false
                let entries = try representedMutations.map { mutation in
                    let evidence = try materializer.baselineEvidence(for: mutation.key) ?? .unknown
                    if evidence == .unknown {
                        containsUnknownBaseline = true
                    }
                    return CollectionConflictEntry(
                        key: mutation.key,
                        operation: mutation.operation,
                        localChanges: mutation.changes,
                        baselineEvidence: evidence
                    )
                }
                let readiness: CollectionConflictRepairReadiness
                if submittedMutations == nil && members.contains(where: { $0.attemptCount > 0 }) {
                    readiness = .unsupportedLegacySubmission
                } else if containsUnknownBaseline {
                    readiness = .requiresAuthoritativeRecovery
                } else {
                    readiness = .ready
                }

                return CollectionConflict(
                    id: groupID,
                    transactionIDs: members.map(\.id),
                    error: leader.lastErrorMessage ?? "Permanent mutation failure",
                    occurredAt: members.compactMap(\.conflictOccurredAt).min()
                        ?? members.map(\.updatedAt).min()
                        ?? Date(),
                    repairReadiness: readiness,
                    entries: entries
                )
            }
            .sorted { lhs, rhs in
                if lhs.occurredAt != rhs.occurredAt {
                    return lhs.occurredAt < rhs.occurredAt
                }
                return lhs.id.uuidString < rhs.id.uuidString
            }
        }
    }

    private func publishConflictSnapshot() {
        guard conflictContinuations.isEmpty == false else { return }
        do {
            let snapshot = try conflictSnapshot()
            for continuation in conflictContinuations.values {
                continuation.yield(snapshot)
            }
        } catch {
            let continuations = Array(conflictContinuations.values)
            conflictContinuations.removeAll()
            continuations.forEach { $0.finish(throwing: error) }
        }
    }

    private func removeConflictContinuation(_ id: UUID) {
        conflictContinuations.removeValue(forKey: id)
    }

    private static func transactionIsEarlier(
        _ lhs: PendingCollectionTransaction,
        _ rhs: PendingCollectionTransaction
    ) -> Bool {
        if lhs.sequenceNumber != rhs.sequenceNumber {
            return lhs.sequenceNumber < rhs.sequenceNumber
        }
        if lhs.createdAt != rhs.createdAt {
            return lhs.createdAt < rhs.createdAt
        }
        return lhs.id.uuidString < rhs.id.uuidString
    }

    private static func mutationIsEarlier(
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
        invalidateQueueContext()
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

    /// Fresh write contexts are used for every atomic transition, while the
    /// queue context is retained for inexpensive eligibility reads. SwiftData
    /// does not automatically invalidate objects already registered in that
    /// context when another context commits, so explicitly roll it back after
    /// each fresh-context transition (including failures).
    private func invalidateQueueContext() {
        queue.context.rollback()
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
