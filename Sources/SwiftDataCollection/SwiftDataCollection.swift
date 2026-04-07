import Foundation
import SwiftData

package enum CollectionMutationCompletion: Sendable, Hashable, Codable {
    case immediate
    case awaitTokens(Set<String>)
    case refresh
}

public struct CollectionMutation: Sendable, Hashable {
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

package typealias CollectionAdapterMutationHandler<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> = @Sendable (CollectionMutationContext<Model, ID>) async throws -> CollectionMutationCompletion

public enum CollectionTransactionStatus: Sendable, Hashable, Codable {
    case durablyQueued
    case sending
    case awaitingSync
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

    public func awaitCompletion() async throws {
        switch statusStorage {
        case .completed:
            return
        case .failed(let message):
            throw CollectionTransactionFailure(message: message)
        case .durablyQueued, .sending, .awaitingSync:
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

    public func markAwaitingSync() {
        statusStorage = .awaitingSync
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
    package let writeTracer: CollectionWriteTracer
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

public struct CollectionOptions<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: Sendable {
    public let debugName: String
    public let modelName: String
    public let identifier: CollectionModelIdentifier<Model, ID>
    public let adapter: CollectionAdapter<Model, ID>
    package let onInsert: CollectionAdapterMutationHandler<Model, ID>?
    package let onUpdate: CollectionAdapterMutationHandler<Model, ID>?
    package let onDelete: CollectionAdapterMutationHandler<Model, ID>?

    public init(
        debugName: String,
        identifier: CollectionModelIdentifier<Model, ID>,
        modelName: String = String(reflecting: Model.self),
        adapter: CollectionAdapter<Model, ID>,
        onInsert: CollectionMutationHandler<Model, ID>? = nil,
        onUpdate: CollectionMutationHandler<Model, ID>? = nil,
        onDelete: CollectionMutationHandler<Model, ID>? = nil
    ) {
        self.init(
            debugName: debugName,
            identifier: identifier,
            modelName: modelName,
            adapter: adapter,
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
        onInsert: CollectionAdapterMutationHandler<Model, ID>? = nil,
        onUpdate: CollectionAdapterMutationHandler<Model, ID>? = nil,
        onDelete: CollectionAdapterMutationHandler<Model, ID>? = nil
    ) {
        self.debugName = debugName
        self.modelName = modelName
        self.identifier = identifier
        self.adapter = adapter
        self.onInsert = onInsert
        self.onUpdate = onUpdate
        self.onDelete = onDelete
    }
}

public struct SwiftDataCollection<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>: Sendable {
    private let startClosure: @Sendable () async -> Void
    private let stopClosure: @Sendable () async -> Void
    private let refreshClosure: @Sendable () async -> Void
    private let statusClosure: @Sendable () async -> CollectionLifecycleState
    private let transactionClosure: @Sendable (@escaping @Sendable (CollectionTransactionBuilder<Model, ID>) throws -> Void) async throws -> CollectionTransaction
    private let insertClosure: @Sendable (@escaping @Sendable () throws -> Model, [String: CollectionValue]) async throws -> CollectionTransaction
    private let updateClosure: @Sendable (ID, [String: CollectionValue], @escaping @Sendable (Model) throws -> Void) async throws -> CollectionTransaction
    private let deleteClosure: @Sendable (ID, [String: CollectionValue]) async throws -> CollectionTransaction

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
        self.statusClosure = { await coordinator.status() }
        self.transactionClosure = { body in try await coordinator.transaction(body) }
        self.insertClosure = { build, metadata in try await coordinator.insert(build, metadata: metadata) }
        self.updateClosure = { key, metadata, mutate in
            try await coordinator.update(key, metadata: metadata, mutate)
        }
        self.deleteClosure = { key, metadata in
            try await coordinator.delete(key, metadata: metadata)
        }
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
}
