import ElectricSwift
import Foundation
import SwiftData
import SwiftDataCollection

public enum ElectricMutationSubmission: Sendable, Hashable, Codable {
    case immediate
    case txid(Int64)
    case txids(Set<Int64>)

    public init(awaitedTXIDs txids: [Int64]) {
        self = .txids(Set(txids))
    }
}

public typealias ElectricMutationHandler<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> = @Sendable (CollectionMutationContext<Model, ID>) async throws -> ElectricMutationSubmission

public struct ElectricCollectionOptions<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>: Sendable {
    public let debugName: String
    public let identifier: CollectionModelIdentifier<Model, ID>
    public let modelName: String
    public let shapeURL: URL
    public let table: String
    public let shapeID: String?
    public let columns: [String]
    public let whereClause: String?
    public let replica: ElectricReplica
    public let headers: [String: String]
    public let extraParameters: [String: String]
    public let utilities: ElectricCollectionSyncUtilities
    public let onInsert: ElectricMutationHandler<Model, ID>?
    public let onUpdate: ElectricMutationHandler<Model, ID>?
    public let onDelete: ElectricMutationHandler<Model, ID>?

    public init(
        debugName: String? = nil,
        identifier: CollectionModelIdentifier<Model, ID>,
        modelName: String = String(reflecting: Model.self),
        shapeURL: URL,
        table: String,
        shapeID: String? = nil,
        columns: [String] = [],
        where whereClause: String? = nil,
        replica: ElectricReplica = .default,
        headers: [String: String] = [:],
        extraParameters: [String: String] = [:],
        utilities: ElectricCollectionSyncUtilities = ElectricCollectionSyncUtilities(),
        onInsert: ElectricMutationHandler<Model, ID>? = nil,
        onUpdate: ElectricMutationHandler<Model, ID>? = nil,
        onDelete: ElectricMutationHandler<Model, ID>? = nil
    ) {
        let resolvedShapeID = Self.resolvedShapeID(
            explicitShapeID: shapeID,
            table: table,
            whereClause: whereClause
        )
        self.debugName = debugName ?? resolvedShapeID
        self.identifier = identifier
        self.modelName = modelName
        self.shapeURL = shapeURL
        self.table = table
        self.shapeID = shapeID
        self.columns = columns
        self.whereClause = whereClause
        self.replica = replica
        self.headers = headers
        self.extraParameters = extraParameters
        self.utilities = utilities
        self.onInsert = onInsert
        self.onUpdate = onUpdate
        self.onDelete = onDelete
    }

    public func collectionOptions() -> CollectionOptions<Model, ID> {
        let resolvedShapeID = Self.resolvedShapeID(
            explicitShapeID: shapeID,
            table: table,
            whereClause: whereClause
        )
        let adapter = CollectionAdapter<Model, ID>(
            sourceID: resolvedShapeID,
            makeRuntime: { context in
                await ElectricCollectionAdapterRuntime.make(
                    configuration: self,
                    context: context
                )
            }
        )

        return CollectionOptions(
            debugName: debugName,
            identifier: identifier,
            modelName: modelName,
            adapter: adapter,
            onInsert: Self.wrap(onInsert),
            onUpdate: Self.wrap(onUpdate),
            onDelete: Self.wrap(onDelete)
        )
    }

    private static func wrap(
        _ handler: ElectricMutationHandler<Model, ID>?
    ) -> CollectionAdapterMutationHandler<Model, ID>? {
        guard let handler else { return nil }

        return { context in
            switch try await handler(context) {
            case .immediate:
                return .immediate
            case .txid(let txid):
                return .awaitTokens([String(txid)])
            case .txids(let txids):
                return .awaitTokens(Set(txids.map(String.init)))
            }
        }
    }

    static func resolvedShapeID(
        explicitShapeID: String?,
        table: String,
        whereClause: String?
    ) -> String {
        if let explicitShapeID {
            return explicitShapeID
        }
        if let whereClause, whereClause.isEmpty == false {
            return "\(table):\(whereClause)"
        }
        return table
    }
}

public func electricCollectionOptions<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>(
    debugName: String? = nil,
    identifier: CollectionModelIdentifier<Model, ID>,
    shapeURL: URL,
    table: String,
    shapeID: String? = nil,
    columns: [String] = [],
    where whereClause: String? = nil,
    replica: ElectricReplica = .default,
    headers: [String: String] = [:],
    extraParameters: [String: String] = [:],
    utilities: ElectricCollectionSyncUtilities = ElectricCollectionSyncUtilities(),
    onInsert: ElectricMutationHandler<Model, ID>? = nil,
    onUpdate: ElectricMutationHandler<Model, ID>? = nil,
    onDelete: ElectricMutationHandler<Model, ID>? = nil
) -> CollectionOptions<Model, ID> {
    ElectricCollectionOptions(
        debugName: debugName,
        identifier: identifier,
        shapeURL: shapeURL,
        table: table,
        shapeID: shapeID,
        columns: columns,
        where: whereClause,
        replica: replica,
        headers: headers,
        extraParameters: extraParameters,
        utilities: utilities,
        onInsert: onInsert,
        onUpdate: onUpdate,
        onDelete: onDelete
    )
    .collectionOptions()
}

actor ElectricCollectionAdapterRuntime<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
>: CollectionAdapterRuntime, ElectricShapeStoreObserver {
    private let reportApplied: @Sendable (Set<String>, Date?, String?) async -> Void
    private let reportError: @Sendable (Error) async -> Void
    private let utilities: ElectricCollectionSyncUtilities
    private let shapeStore: ElectricShapeStore
    private let subscription: ElectricShapeSubscription<Model>

    private init(
        reportApplied: @escaping @Sendable (Set<String>, Date?, String?) async -> Void,
        reportError: @escaping @Sendable (Error) async -> Void,
        utilities: ElectricCollectionSyncUtilities,
        shapeStore: ElectricShapeStore,
        subscription: ElectricShapeSubscription<Model>
    ) {
        self.reportApplied = reportApplied
        self.reportError = reportError
        self.utilities = utilities
        self.shapeStore = shapeStore
        self.subscription = subscription
    }

    static func make(
        configuration: ElectricCollectionOptions<Model, ID>,
        context: CollectionAdapterContext<Model, ID>
    ) async -> ElectricCollectionAdapterRuntime<Model, ID> {
        let shapeStore = ElectricShapeStore(
            shapeURL: configuration.shapeURL,
            modelContainer: context.modelContainer,
            rowDecoder: context.rowDecoder,
            debugLogger: context.debugLogger.electricDebugLogger,
            observer: nil
        )
        let synchronizer = ElectricCollectionSynchronizer<Model, ID>(
            identifier: context.identifier,
            rowDecoder: context.rowDecoder,
            modelName: configuration.modelName,
            collectionID: context.collectionID,
            writeTracer: context.writeTracer,
            debugLogger: context.debugLogger.electricDebugLogger
        )
        let resolvedShapeID = ElectricCollectionOptions<Model, ID>.resolvedShapeID(
            explicitShapeID: configuration.shapeID,
            table: configuration.table,
            whereClause: configuration.whereClause
        )
        let subscription = await shapeStore.shape(
            Model.self,
            identifier: context.identifier,
            table: configuration.table,
            shapeID: resolvedShapeID,
            columns: configuration.columns,
            where: configuration.whereClause,
            replica: configuration.replica,
            headers: configuration.headers,
            extraParameters: configuration.extraParameters,
            batchApplier: { batch, shapeID, modelContext in
                try synchronizer.apply(batch, shapeID: shapeID, in: modelContext)
            }
        )

        let runtime = ElectricCollectionAdapterRuntime(
            reportApplied: context.reportApplied,
            reportError: context.reportError,
            utilities: configuration.utilities,
            shapeStore: shapeStore,
            subscription: subscription
        )
        await shapeStore.setObserver(runtime)
        return runtime
    }

    func start() async {
        await subscription.start()
    }

    func stop() async {
        await subscription.stop()
    }

    func refresh() async {
        await subscription.refresh()
    }

    func shapeStoreDidApply(
        batch: ShapeBatch,
        shapeID: String,
        resolvedTransactionIDs: [UUID]
    ) async {
        let observedTokens = Set(batch.messages.flatMap { $0.headers.txids ?? [] }.map(String.init))
        await reportApplied(observedTokens, batch.state.lastSyncedAt, batch.state.offset)
        await utilities.observeAppliedBatch(batch)
    }
}

public extension SwiftDataCollection {
    var shapeID: String { sourceID }
}

public extension SwiftDataCollectionStore {
    init(
        shapeURL _: URL,
        modelContainer: ModelContainer,
        rowDecoder: CollectionRowDecoder = .init(),
        debugLogger: CollectionDebugLogger = .disabled,
        writeTracer: CollectionWriteTracer = .disabled,
        commitSave: @escaping @Sendable (ModelContext) throws -> Void = { try $0.save() },
        retryPolicy: any PendingMutationRetryDelaying = CollectionRetryPolicy()
    ) {
        self.init(
            modelContainer: modelContainer,
            rowDecoder: rowDecoder,
            debugLogger: debugLogger,
            writeTracer: writeTracer,
            commitSave: commitSave,
            retryPolicy: retryPolicy
        )
    }

    func shapeStoreDidApply(
        batch: ShapeBatch,
        shapeID: String,
        resolvedTransactionIDs _: [UUID]
    ) async {
        await reportAdapterApplied(
            sourceID: shapeID,
            observedTokens: Set(batch.messages.flatMap { $0.headers.txids ?? [] }.map(String.init)),
            lastSyncedAt: batch.state.lastSyncedAt,
            offset: batch.state.offset
        )
    }

    func collection<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>(
        _ model: Model.Type,
        identifier: CollectionModelIdentifier<Model, ID>,
        shapeURL: URL,
        table: String,
        shapeID: String? = nil,
        columns: [String] = [],
        where whereClause: String? = nil,
        replica: ElectricReplica = .default,
        headers: [String: String] = [:],
        extraParameters: [String: String] = [:],
        debugName: String? = nil,
        utilities: ElectricCollectionSyncUtilities = ElectricCollectionSyncUtilities(),
        onInsert: ElectricMutationHandler<Model, ID>? = nil,
        onUpdate: ElectricMutationHandler<Model, ID>? = nil,
        onDelete: ElectricMutationHandler<Model, ID>? = nil
    ) async throws -> SwiftDataCollection<Model, ID> {
        try await collection(
            model,
            options: electricCollectionOptions(
                debugName: debugName,
                identifier: identifier,
                shapeURL: shapeURL,
                table: table,
                shapeID: shapeID,
                columns: columns,
                where: whereClause,
                replica: replica,
                headers: headers,
                extraParameters: extraParameters,
                utilities: utilities,
                onInsert: onInsert,
                onUpdate: onUpdate,
                onDelete: onDelete
            )
        )
    }

    func shape<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>(
        _ model: Model.Type,
        identifier: CollectionModelIdentifier<Model, ID>,
        shapeURL: URL,
        table: String,
        shapeID: String? = nil,
        columns: [String] = [],
        where whereClause: String? = nil,
        replica: ElectricReplica = .default,
        headers: [String: String] = [:],
        extraParameters: [String: String] = [:]
    ) async throws -> ElectricShapeSubscription<Model> {
        let modelName = String(reflecting: Model.self)
        let resolvedShapeID = ElectricCollectionOptions<Model, ID>.resolvedShapeID(
            explicitShapeID: shapeID,
            table: table,
            whereClause: whereClause
        )
        let descriptor = CollectionManagedSourceDescriptor(sourceID: resolvedShapeID)

        if let existing = existingRegistration(modelName: modelName) {
            guard existing.kind == .shape, existing.descriptor == descriptor else {
                throw SwiftDataCollectionStoreError.managedShapeConflict(
                    modelName: modelName,
                    existingKind: existing.kind,
                    existingShapeID: existing.descriptor.sourceID,
                    requestedKind: .shape,
                    requestedShapeID: descriptor.sourceID
                )
            }
            guard let subscription = existing.shapeFactory?.make() as? ElectricShapeSubscription<Model> else {
                throw SwiftDataCollectionStoreError.managedShapeConflict(
                    modelName: modelName,
                    existingKind: existing.kind,
                    existingShapeID: existing.descriptor.sourceID,
                    requestedKind: .shape,
                    requestedShapeID: descriptor.sourceID
                )
            }
            return subscription
        }

        let shapeStore = ElectricShapeStore(
            shapeURL: shapeURL,
            modelContainer: storeModelContainer,
            rowDecoder: storeRowDecoder,
            debugLogger: storeDebugLogger.electricDebugLogger,
            observer: nil
        )
        let subscription = await shapeStore.shape(
            model,
            identifier: identifier,
            table: table,
            shapeID: resolvedShapeID,
            columns: columns,
            where: whereClause,
            replica: replica,
            headers: headers,
            extraParameters: extraParameters,
            batchApplier: nil
        )
        register(
            CollectionManagedModelRegistration(
                modelName: modelName,
                kind: .shape,
                descriptor: descriptor,
                debugName: nil,
                shapeFactory: CollectionManagedShapeFactory {
                    subscription
                },
                collectionFactory: nil
            )
        )
        return subscription
    }

}
