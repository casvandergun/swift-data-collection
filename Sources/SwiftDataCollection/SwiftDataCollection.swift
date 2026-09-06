import Foundation
import SwiftData

package enum CollectionMutationCompletion: Sendable, Hashable, Codable {
    case immediate
    case awaitTokens(Set<String>)
    case refresh
}

public struct CollectionMutation: Sendable, Hashable, Codable {
    public let operation: CollectionMutationOperation
    public let key: String
    public let original: CollectionRow?
    public let modified: CollectionRow?
    public let changes: CollectionRow
    public let metadata: [String: CollectionValue]

    public init(
        operation: CollectionMutationOperation,
        key: String,
        original: CollectionRow? = nil,
        modified: CollectionRow? = nil,
        changes: CollectionRow = [:],
        metadata: [String: CollectionValue] = [:]
    ) {
        self.operation = operation
        self.key = key
        self.original = original
        self.modified = modified
        self.changes = changes
        self.metadata = metadata
    }
}

public struct CollectionMutationContext<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>: Sendable {
    public let transaction: CollectionTransaction
    public let mutations: [CollectionMutation]

    public init(
        transaction: CollectionTransaction,
        mutations: [CollectionMutation]
    ) {
        self.transaction = transaction
        self.mutations = mutations
    }
}

public typealias CollectionMutationHandler<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> = @Sendable (CollectionMutationContext<Model, ID>) async throws -> Void

public struct CollectionBatchApplySummary: Sendable, Hashable {
    public let collectionIdentifier: String
    public let sourceIdentifier: String
    public let insertedCount: Int
    public let updatedCount: Int
    public let deletedCount: Int
    public let observedTXIDs: [Int64]

    public init(
        collectionIdentifier: String,
        sourceIdentifier: String,
        insertedCount: Int,
        updatedCount: Int,
        deletedCount: Int,
        observedTXIDs: [Int64] = []
    ) {
        self.collectionIdentifier = collectionIdentifier
        self.sourceIdentifier = sourceIdentifier
        self.insertedCount = insertedCount
        self.updatedCount = updatedCount
        self.deletedCount = deletedCount
        self.observedTXIDs = Array(Set(observedTXIDs)).sorted()
    }
}

public typealias CollectionApplyHandler =
    @Sendable (_ context: ModelContext, _ summary: CollectionBatchApplySummary) throws -> Void

package typealias CollectionAdapterMutationHandler<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> = @Sendable (CollectionMutationContext<Model, ID>) async throws -> CollectionMutationCompletion

public enum CollectionTransactionStatus: Sendable, Hashable, Codable {
    case durablyQueued
    case sending
    case awaiting
    case completed
    case failed(String)
}

public actor CollectionTransaction {
    public let id: UUID
    public let collectionID: String?

    private var statusStorage: CollectionTransactionStatus = .durablyQueued
    private var waiters: [CheckedContinuation<Void, Error>] = []

    public init(id: UUID = UUID(), collectionID: String? = nil) {
        self.id = id
        self.collectionID = collectionID
    }

    public var status: CollectionTransactionStatus {
        statusStorage
    }

    public func wait() async throws {
        switch statusStorage {
        case .completed:
            return
        case .failed(let message):
            throw CollectionTransactionFailure(message: message)
        case .durablyQueued, .sending, .awaiting:
            try await withCheckedThrowingContinuation { continuation in
                waiters.append(continuation)
            }
        }
    }

    public func markDurablyQueued() {
        statusStorage = .durablyQueued
    }

    public func markSending() {
        statusStorage = .sending
    }

    public func markAwaiting() {
        statusStorage = .awaiting
    }

    public func complete() {
        statusStorage = .completed
        let continuations = waiters
        waiters.removeAll(keepingCapacity: true)
        continuations.forEach { $0.resume() }
    }

    public func fail(_ error: Error) {
        statusStorage = .failed(String(describing: error))
        let continuations = waiters
        waiters.removeAll(keepingCapacity: true)
        continuations.forEach { $0.resume(throwing: error) }
    }
}

public struct CollectionTransactionFailure: Error, Sendable {
    public let message: String

    public init(message: String) {
        self.message = message
    }
}

public enum StagedInsertOutcome: Sendable, Hashable {
    case inserted
    case alreadyStaged
    case alreadySynced
}

package protocol CollectionAdapterRuntime: Actor {
    func start() async
    func stop() async
    func refresh() async
}

package struct CollectionAdapterContext<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: Sendable {
    package let modelContainer: ModelContainer
    package let collectionID: String
    package let sourceID: String
    package let debugName: String
    package let identifier: CollectionModelIdentifier<Model, ID>
    package let rowDecoder: CollectionRowDecoder
    package let debugLogger: CollectionDebugLogger
    package let tracer: CollectionTracer
    package let writeGate: CollectionWriteGate
    package let onApply: CollectionApplyHandler?
    package let reportApplied: @Sendable (Set<String>, Date?, String?) async -> Void
    package let reportRefreshCompleted: @Sendable (Date?) async -> Void
    package let reportError: @Sendable (Error) async -> Void
}

public struct CollectionAdapter<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: Sendable {
    public let sourceID: String

    package let makeRuntime: @Sendable (CollectionAdapterContext<Model, ID>) async throws -> any CollectionAdapterRuntime

    package init(
        sourceID: String,
        makeRuntime: @escaping @Sendable (CollectionAdapterContext<Model, ID>) async throws -> any CollectionAdapterRuntime
    ) {
        self.sourceID = sourceID
        self.makeRuntime = makeRuntime
    }
}

/*
 * How long a write waits before returning.
 *
 * `dispatchAttempted` keeps the original behaviour: the call returns once the
 * outbound dispatch for the transaction has settled, so post-dispatch state
 * (`awaiting`, `conflicted`) is observable the moment the write returns.
 *
 * `durablyQueued` returns as soon as the transaction is committed to the
 * outbox and leaves dispatch to the runtime. Prefer it wherever a write sits
 * on a latency-sensitive path: a local-first write should not wait on a round
 * trip, and on a slow network `dispatchAttempted` makes it do exactly that.
 * Durability is identical either way -- the outbox row is committed before
 * dispatch begins in both modes. Callers that still need the round trip await
 * `CollectionTransaction.wait()`, or drain the collection with `flush()`.
 */
public enum CollectionDispatchWait: String, Sendable, Hashable, Codable {
    case dispatchAttempted
    case durablyQueued
}

public struct CollectionOptions<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: Sendable {
    public let debugName: String
    public let modelName: String
    public let identifier: CollectionModelIdentifier<Model, ID>
    public let adapter: CollectionAdapter<Model, ID>
    public let onApply: CollectionApplyHandler?
    public let dispatchWait: CollectionDispatchWait
    package let onInsert: CollectionAdapterMutationHandler<Model, ID>?
    package let onUpdate: CollectionAdapterMutationHandler<Model, ID>?
    package let onDelete: CollectionAdapterMutationHandler<Model, ID>?

    public init(
        debugName: String,
        identifier: CollectionModelIdentifier<Model, ID>,
        modelName: String = String(reflecting: Model.self),
        adapter: CollectionAdapter<Model, ID>,
        onApply: CollectionApplyHandler? = nil,
        dispatchWait: CollectionDispatchWait = .dispatchAttempted,
        onInsert: CollectionMutationHandler<Model, ID>? = nil,
        onUpdate: CollectionMutationHandler<Model, ID>? = nil,
        onDelete: CollectionMutationHandler<Model, ID>? = nil
    ) {
        self.init(
            debugName: debugName,
            identifier: identifier,
            modelName: modelName,
            adapter: adapter,
            onApply: onApply,
            dispatchWait: dispatchWait,
            onInsert: onInsert.map { handler in
                { @Sendable context in
                    try await handler(context)
                    return .immediate
                }
            },
            onUpdate: onUpdate.map { handler in
                { @Sendable context in
                    try await handler(context)
                    return .immediate
                }
            },
            onDelete: onDelete.map { handler in
                { @Sendable context in
                    try await handler(context)
                    return .immediate
                }
            }
        )
    }

    package init(
        debugName: String,
        identifier: CollectionModelIdentifier<Model, ID>,
        modelName: String = String(reflecting: Model.self),
        adapter: CollectionAdapter<Model, ID>,
        onApply: CollectionApplyHandler? = nil,
        dispatchWait: CollectionDispatchWait = .dispatchAttempted,
        onInsert: CollectionAdapterMutationHandler<Model, ID>? = nil,
        onUpdate: CollectionAdapterMutationHandler<Model, ID>? = nil,
        onDelete: CollectionAdapterMutationHandler<Model, ID>? = nil
    ) {
        self.debugName = debugName
        self.modelName = modelName
        self.identifier = identifier
        self.adapter = adapter
        self.onApply = onApply
        self.dispatchWait = dispatchWait
        self.onInsert = onInsert
        self.onUpdate = onUpdate
        self.onDelete = onDelete
    }
}

public struct SwiftDataCollection<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>: Sendable {
    private let startClosure: @Sendable () async -> Void
    private let stopClosure: @Sendable () async -> Void
    private let refreshClosure: @Sendable () async -> Void
    private let flushClosure: @Sendable () async -> Void
    private let statusClosure: @Sendable () async -> CollectionLifecycleState
    private let transactionClosure: @Sendable (@escaping @Sendable (CollectionTransactionBuilder<Model, ID>) throws -> Void) async throws -> CollectionTransaction
    private let insertClosure: @Sendable (@escaping @Sendable () throws -> Model, [String: CollectionValue]) async throws -> CollectionTransaction
    private let updateClosure: @Sendable (ID, [String: CollectionValue], @escaping @Sendable (Model) throws -> Void) async throws -> CollectionTransaction
    private let deleteClosure: @Sendable (ID, [String: CollectionValue]) async throws -> CollectionTransaction
    private let stageInsertClosure: @Sendable (@escaping @Sendable () throws -> Model) async throws -> StagedInsertOutcome
    private let updateStagedClosure: @Sendable (ID, @escaping @Sendable (Model) throws -> Void) async throws -> Void
    private let publishStagedInsertClosure: @Sendable (ID, [String: CollectionValue]) async throws -> CollectionTransaction
    private let discardStagedInsertClosure: @Sendable (ID) async throws -> Void
    private let conflictsClosure: @Sendable () async throws -> [CollectionConflict]
    private let conflictUpdatesClosure: @Sendable () async -> AsyncThrowingStream<[CollectionConflict], any Error>
    private let discardConflictClosure: @Sendable (UUID) async throws -> Void

    public let sourceID: String
    public let debugName: String

    init(
        coordinator: CollectionCoordinator<Model, ID>,
        sourceID: String,
        debugName: String
    ) {
        self.startClosure = { await coordinator.start() }
        self.stopClosure = { await coordinator.stop() }
        self.refreshClosure = { await coordinator.refresh() }
        self.flushClosure = { await coordinator.flush() }
        self.statusClosure = { await coordinator.status() }
        self.transactionClosure = { body in try await coordinator.transaction(body) }
        self.insertClosure = { build, metadata in try await coordinator.insert(build, metadata: metadata) }
        self.updateClosure = { key, metadata, mutate in
            try await coordinator.update(key, metadata: metadata, mutate)
        }
        self.deleteClosure = { key, metadata in
            try await coordinator.delete(key, metadata: metadata)
        }
        self.stageInsertClosure = { build in try await coordinator.stageInsert(build) }
        self.updateStagedClosure = { key, mutate in try await coordinator.updateStaged(key, mutate) }
        self.publishStagedInsertClosure = { key, metadata in
            try await coordinator.publishStagedInsert(key, metadata: metadata)
        }
        self.discardStagedInsertClosure = { key in try await coordinator.discardStagedInsert(key) }
        self.conflictsClosure = { try await coordinator.conflicts() }
        self.conflictUpdatesClosure = { await coordinator.conflictUpdates() }
        self.discardConflictClosure = { conflictID in try await coordinator.discard(conflictID) }
        self.sourceID = sourceID
        self.debugName = debugName
    }

    public func start() async {
        await startClosure()
    }

    public func stop() async {
        await stopClosure()
    }

    public func refresh() async {
        await refreshClosure()
    }

    /// Drains any transactions waiting to be dispatched and returns once the
    /// drain settles.
    ///
    /// Writes return as soon as they are durably queued, so this is how a
    /// caller that must observe outbound dispatch -- a test, or a shutdown
    /// path -- waits for it. Awaiting a single transaction's round trip is
    /// `CollectionTransaction.wait()`.
    public func flush() async {
        await flushClosure()
    }

    public var status: CollectionLifecycleState {
        get async { await statusClosure() }
    }

    public func transaction(
        _ body: @escaping @Sendable (CollectionTransactionBuilder<Model, ID>) throws -> Void
    ) async throws -> CollectionTransaction {
        try await transactionClosure(body)
    }

    public func insert(
        _ build: @escaping @Sendable () throws -> Model,
        metadata: [String: CollectionValue] = [:]
    ) async throws -> CollectionTransaction {
        try await insertClosure(build, metadata)
    }

    public func update(
        _ key: ID,
        metadata: [String: CollectionValue] = [:],
        _ mutate: @escaping @Sendable (Model) throws -> Void
    ) async throws -> CollectionTransaction {
        try await updateClosure(key, metadata, mutate)
    }

    public func delete(
        _ key: ID,
        metadata: [String: CollectionValue] = [:]
    ) async throws -> CollectionTransaction {
        try await deleteClosure(key, metadata)
    }

    public func stageInsert(
        _ build: @escaping @Sendable () throws -> Model
    ) async throws -> StagedInsertOutcome {
        try await stageInsertClosure(build)
    }

    public func updateStaged(
        _ key: ID,
        _ mutate: @escaping @Sendable (Model) throws -> Void
    ) async throws {
        try await updateStagedClosure(key, mutate)
    }

    public func publishStagedInsert(
        _ key: ID,
        metadata: [String: CollectionValue] = [:]
    ) async throws -> CollectionTransaction {
        try await publishStagedInsertClosure(key, metadata)
    }

    public func discardStagedInsert(_ key: ID) async throws {
        try await discardStagedInsertClosure(key)
    }

    /// Returns the currently parked permanent-failure groups for this collection.
    public func conflicts() async throws -> [CollectionConflict] {
        try await conflictsClosure()
    }

    /// Complete conflict snapshots, beginning with the current committed state.
    /// Each access creates an independent newest-value-buffered subscription.
    public var conflictUpdates: AsyncThrowingStream<[CollectionConflict], any Error> {
        get async { await conflictUpdatesClosure() }
    }

    /// Abandons all local intent in a conflicted dispatch group and rebuilds the
    /// visible rows from retained authoritative evidence plus surviving intent.
    public func discard(_ conflictID: UUID) async throws {
        try await discardConflictClosure(conflictID)
    }
}
