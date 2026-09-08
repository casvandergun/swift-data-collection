import Foundation
import SwiftData

/*
 * Store-wide dispatch ordering.
 *
 * Collections are independent state engines for lifecycle and row
 * materialization, but the application records one ordered stream of writes.
 * Scheduling each collection separately let a child's mutation reach the
 * server before its parent's, so the store owns a single durable FIFO lane
 * that dispatches across every registered collection in sequence order and
 * routes each transaction back to its own collection's mutation handlers.
 *
 * The lane releases as soon as the outbound handler returns and the group is
 * durably `awaiting`/`resolved`. Waiting for adapter readback instead would
 * couple every later write in the store to txid latency, and server
 * acceptance is already enough to order a child behind its parent.
 */

/// Durable store-wide transaction sequence allocator.
///
/// Ordering must survive relaunch, so the counter is a persisted row rather
/// than in-memory state. A `ModelContainer` is the store, so there is exactly
/// one of these per container.
@Model
public final class CollectionStoreMetadata {
    @Attribute(.unique) public var storeID: String
    package var nextTransactionSequence: Int = 0

    public init(storeID: String = "default") {
        self.storeID = storeID
    }
}

/// What one lane-driven dispatch attempt did to durable state.
package enum CollectionDispatchOutcome: Sendable, Hashable {
    /// The outbound handler ran and durable state advanced. Re-evaluate.
    case dispatched
    /// This transaction cannot dispatch right now, but the collection can.
    /// Step past it and leave it for a later pass.
    case skipped
    /// The whole collection cannot dispatch right now. Hold the lane.
    ///
    /// Distinct from `skipped` because stepping past a collection that is
    /// merely paused -- offline, say -- reorders every one of its transactions
    /// behind work that was recorded after them.
    case deferred
    /// Durable state could not be persisted. Abandon this drain.
    case halted
}

package actor CollectionDispatchLane {
    private struct Entry {
        let id: UUID
        let collectionID: String
        let shapeID: String
        let modelName: String
        let order: CollectionTransactionOrder
        let status: PendingTransactionState
        let attemptCount: Int
        let nextRetryAt: Date?

        var sequenceNumber: Int { order.sequenceNumber }
    }

    private let modelContainer: ModelContainer
    private let retrySleep: CollectionRetrySleeper
    private let tracer: CollectionTracer

    private var runtimesByCollectionID: [String: any CollectionRuntime] = [:]
    private var connectivityState: CollectionConnectivityState
    private var isDraining = false
    private var drainRequestedAgain = false
    private var drainWaiters: [CheckedContinuation<Void, Never>] = []
    private var attemptWaiters: [UUID: [CheckedContinuation<Void, Never>]] = [:]
    private var scheduledRetryAt: Date?
    private var scheduledRetryTask: Task<Void, Never>?

    package init(
        modelContainer: ModelContainer,
        retrySleep: @escaping CollectionRetrySleeper = defaultCollectionRetrySleep,
        tracer: CollectionTracer,
        connectivityState: CollectionConnectivityState = .online
    ) {
        self.modelContainer = modelContainer
        self.retrySleep = retrySleep
        self.tracer = tracer
        self.connectivityState = connectivityState
    }

    deinit {
        scheduledRetryTask?.cancel()
    }

    package func register(_ runtime: any CollectionRuntime, collectionID: String) {
        runtimesByCollectionID[collectionID] = runtime
    }

    package func setConnectivityState(_ state: CollectionConnectivityState) {
        guard connectivityState != state else { return }
        connectivityState = state
        if state == .offline {
            cancelScheduledRetry()
        }
    }

    /*
     * Waiting for one transaction is not the same as waiting for the lane.
     *
     * A `.dispatchAttempted` write wants to know its own mutation reached a
     * handler. The lane drains until it quiesces, so making that caller await
     * the whole drain binds an interactive write to every transaction the store
     * happens to be carrying -- including work queued after it, in collections
     * it knows nothing about.
     */
    package func drain(untilAttempted transactionID: UUID) async {
        await withCheckedContinuation { continuation in
            attemptWaiters[transactionID, default: []].append(continuation)
            Task { await self.drain() }
        }
    }

    private func resumeAttemptWaiters(for transactionID: UUID) {
        guard let waiters = attemptWaiters.removeValue(forKey: transactionID) else { return }
        for waiter in waiters {
            waiter.resume()
        }
    }

    /// Resumes every waiter whose transaction has left the dispatchable set,
    /// which covers members compacted into another transaction's request.
    private func resumeAttemptWaitersAbsent(from entries: [Entry]) {
        guard attemptWaiters.isEmpty == false else { return }
        let present = Set(entries.map(\.id))
        for id in attemptWaiters.keys where present.contains(id) == false {
            resumeAttemptWaiters(for: id)
        }
    }

    private func resumeAllAttemptWaiters() {
        let waiters = attemptWaiters.values.flatMap { $0 }
        attemptWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }

    /// Drains the lane, and guarantees that work durably queued before this
    /// call has been given a dispatch attempt before it returns.
    ///
    /// A caller arriving mid-drain cannot simply await the running pass: that
    /// pass may already be past the point where it would have seen the new
    /// transaction. It instead requests another pass and waits for that.
    package func drain() async {
        guard isDraining == false else {
            drainRequestedAgain = true
            await withCheckedContinuation { drainWaiters.append($0) }
            return
        }

        isDraining = true
        repeat {
            drainRequestedAgain = false
            await runDrainPass()
        } while drainRequestedAgain
        isDraining = false

        // Nothing further will be attempted in this drain, so a waiter still
        // outstanding is either blocked behind a hold or not dispatchable at
        // all. Either way it has its answer.
        resumeAllAttemptWaiters()

        let waiters = drainWaiters
        drainWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func runDrainPass() async {
        guard connectivityState == .online else {
            cancelScheduledRetry()
            return
        }

        // Transactions stepped past in this pass because they are blocked
        // behind an unresolved same-key predecessor. They are re-considered as
        // soon as any dispatch advances durable state.
        var steppedPast: Set<UUID> = []

        while true {
            guard connectivityState == .online else {
                cancelScheduledRetry()
                return
            }

            let entries = laneEntries()
            guard let head = entries.first(where: { steppedPast.contains($0.id) == false }) else {
                scheduleNextRetry(among: entries)
                return
            }

            if head.status == .sending {
                // Its collection has not bootstrapped, so nothing has reset it
                // for replay yet and the lane cannot know whether the work
                // reached the server. Hold; bootstrap drains again.
                guard runtimesByCollectionID[head.collectionID] != nil else {
                    trace(
                        .dispatchDeferred,
                        entry: head,
                        message: "lane holding behind persisted in-flight work awaiting bootstrap",
                        metadata: ["unregisteredCollectionID": head.collectionID]
                    )
                    return
                }
                // A registered collection already reset its crash remnants, so
                // this belongs to a dispatch the coordinator still owns.
                steppedPast.insert(head.id)
                continue
            }

            if let retryAt = head.nextRetryAt, retryAt > Date() {
                // Strict FIFO: a retryable head holds the lane rather than
                // letting later work overtake it.
                scheduleRetry(at: retryAt, head: head)
                return
            }

            /*
             * An app materializes its collections one at a time at launch, so
             * the earliest pending transaction can belong to a collection that
             * has not registered yet. Stepping past it would let a child
             * replay ahead of its parent across a restart -- the original bug,
             * reappearing on exactly the path that is hardest to observe. Hold
             * instead; registering the collection drains again.
             */
            guard let runtime = runtimesByCollectionID[head.collectionID] else {
                trace(
                    .dispatchDeferred,
                    entry: head,
                    message: "lane holding for a collection that has not registered yet",
                    metadata: ["unregisteredCollectionID": head.collectionID]
                )
                return
            }

            cancelScheduledRetry()
            switch await runtime.laneDispatch(transactionID: head.id) {
            case .dispatched:
                resumeAttemptWaiters(for: head.id)
                resumeAttemptWaitersAbsent(from: laneEntries())
                steppedPast.removeAll()
            case .skipped:
                trace(
                    .dispatchDeferred,
                    entry: head,
                    message: "stepped past transaction that cannot dispatch yet"
                )
                steppedPast.insert(head.id)
            case .deferred:
                trace(
                    .dispatchDeferred,
                    entry: head,
                    message: "lane holding for a collection that cannot dispatch yet",
                    metadata: ["pausedCollectionID": head.collectionID]
                )
                return
            case .halted:
                return
            }
        }
    }

    /*
     * Only group leaders participate; compacted members travel with their
     * leader's request. `conflicted` and `discarded` are deliberately absent:
     * this package parks permanently refused intent instead of dropping it, so
     * a terminal transaction would otherwise block every collection in the
     * store forever.
     *
     * `sending` is present but never dispatchable. A process that died mid
     * dispatch leaves that state on disk, and its collection rewrites it to
     * `pending` during bootstrap -- so until that collection exists again, the
     * record is the only evidence that earlier work is still outstanding, and
     * the lane has to hold behind it rather than run its successors.
     */
    private func laneEntries() -> [Entry] {
        let context = ModelContext(modelContainer)
        let transactions = (try? context.fetch(FetchDescriptor<PendingCollectionTransaction>())) ?? []
        return transactions
            .filter { $0.status == .pending || $0.status == .failed || $0.status == .sending }
            .filter { $0.dispatchGroupID == nil || $0.dispatchGroupID == $0.id }
            .map {
                Entry(
                    id: $0.id,
                    collectionID: $0.collectionID,
                    shapeID: $0.shapeID,
                    modelName: $0.modelName,
                    order: CollectionTransactionOrder($0),
                    status: $0.status,
                    attemptCount: $0.attemptCount,
                    nextRetryAt: $0.nextRetryAt
                )
            }
            .sorted { $0.order < $1.order }
    }

    private func scheduleNextRetry(among entries: [Entry]) {
        let now = Date()
        let next = entries
            .compactMap { entry -> (Date, Entry)? in
                guard let nextRetryAt = entry.nextRetryAt, nextRetryAt > now else { return nil }
                return (nextRetryAt, entry)
            }
            .min { $0.0 < $1.0 }

        guard let next else {
            cancelScheduledRetry()
            return
        }
        scheduleRetry(at: next.0, head: next.1)
    }

    private func scheduleRetry(at date: Date, head: Entry) {
        guard connectivityState == .online else {
            cancelScheduledRetry()
            return
        }
        guard scheduledRetryAt != date else { return }

        cancelScheduledRetry()
        scheduledRetryAt = date
        let delay = max(0, date.timeIntervalSince(Date()))
        trace(
            .retryScheduled,
            entry: head,
            message: "lane holding for earliest retryable transaction",
            metadata: ["delay": String(delay)]
        )
        let retrySleep = self.retrySleep
        scheduledRetryTask = Task { [weak self] in
            await retrySleep(delay)
            guard Task.isCancelled == false else { return }
            await self?.scheduledRetryDidFire(expectedRetryAt: date)
        }
    }

    private func scheduledRetryDidFire(expectedRetryAt: Date) async {
        guard scheduledRetryAt == expectedRetryAt else { return }
        scheduledRetryAt = nil
        scheduledRetryTask = nil
        await drain()
    }

    private func cancelScheduledRetry() {
        scheduledRetryTask?.cancel()
        scheduledRetryTask = nil
        scheduledRetryAt = nil
    }

    private func trace(
        _ kind: CollectionTraceEventKind,
        entry: Entry,
        message: String,
        metadata: [String: String] = [:]
    ) {
        tracer.record(
            CollectionTraceEvent(
                kind: kind,
                collectionID: entry.collectionID,
                shapeID: entry.shapeID,
                modelName: entry.modelName,
                transactionID: entry.id,
                sequenceNumber: entry.sequenceNumber,
                attemptCount: entry.attemptCount,
                message: message,
                metadata: metadata
            )
        )
    }
}

extension CollectionDispatchLane {
    /// Allocates the next store-wide sequence number.
    ///
    /// Callers must already hold the store's write gate, so allocation and the
    /// caller's durable commit land in the same critical section. Allocating
    /// outside it would let a later transaction commit first and be dispatched
    /// ahead of an earlier one still mid-commit.
    package static func allocateSequenceNumber(in context: ModelContext) throws -> Int {
        let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
        let maximumSequence = transactions.map(\.sequenceNumber).max() ?? -1
        guard maximumSequence < Int.max else {
            throw CollectionError.transactionSequenceOverflow
        }

        let metadata: CollectionStoreMetadata
        if let existing = try context.fetch(FetchDescriptor<CollectionStoreMetadata>()).first {
            metadata = existing
        } else {
            metadata = CollectionStoreMetadata()
            context.insert(metadata)
        }

        let seededNext = max(metadata.nextTransactionSequence, maximumSequence + 1)
        guard seededNext < Int.max else {
            throw CollectionError.transactionSequenceOverflow
        }
        metadata.nextTransactionSequence = seededNext + 1
        return seededNext
    }
}
