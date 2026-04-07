import ElectricSwift
import Foundation
import SwiftData
import SwiftDataCollection

public enum ElectricSubscriptionStatus: Sendable, Hashable, Codable {
    case idle
    case syncing
    case paused
    case upToDate
    case error(String)
}

public protocol ElectricShapeStoreObserver: Actor {
    func shapeStoreDidApply(
        batch: ShapeBatch,
        shapeID: String,
        resolvedTransactionIDs: [UUID]
    ) async
}

public actor ElectricShapeStore {
    typealias SessionFactory = @Sendable (ShapeStreamOptions, ShapeStreamState, ElectricDebugLogger) -> ShapeStream

    private let shapeURL: URL
    private let modelContainer: ModelContainer
    private let session: URLSession
    private let rowDecoder: CollectionRowDecoder
    private let debugLogger: ElectricDebugLogger
    private let sessionFactory: SessionFactory
    private var observer: (any ElectricShapeStoreObserver)?
    private var batchAppliers: [String: ElectricShapeBatchApplyClosure] = [:]

    private var runningTasks: [String: Task<Void, Never>] = [:]
    private var sessions: [String: ShapeStream] = [:]
    private var statuses: [String: ElectricSubscriptionStatus] = [:]
    private var streamStates: [String: ShapeStreamState] = [:]

    public init(
        shapeURL: URL,
        modelContainer: ModelContainer,
        session: URLSession = .shared,
        rowDecoder: CollectionRowDecoder = .init(),
        debugLogger: ElectricDebugLogger = .disabled,
        observer: (any ElectricShapeStoreObserver)? = nil
    ) {
        self.shapeURL = shapeURL
        self.modelContainer = modelContainer
        self.session = session
        self.rowDecoder = rowDecoder
        self.debugLogger = debugLogger
        self.sessionFactory = { options, initialState, debugLogger in
            ShapeStream(
                options: options,
                configuration: .init(initialState: initialState),
                session: session,
                debugLogger: debugLogger
            )
        }
        self.observer = observer
    }

    init(
        shapeURL: URL,
        modelContainer: ModelContainer,
        session: URLSession = .shared,
        rowDecoder: CollectionRowDecoder = .init(),
        debugLogger: ElectricDebugLogger = .disabled,
        observer: (any ElectricShapeStoreObserver)? = nil,
        sessionFactory: @escaping SessionFactory
    ) {
        self.shapeURL = shapeURL
        self.modelContainer = modelContainer
        self.session = session
        self.rowDecoder = rowDecoder
        self.debugLogger = debugLogger
        self.sessionFactory = sessionFactory
        self.observer = observer
    }

    public func setObserver(_ observer: any ElectricShapeStoreObserver) {
        self.observer = observer
    }

    public func shape<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>(
        _ model: Model.Type,
        identifier: CollectionModelIdentifier<Model, ID>,
        table: String,
        shapeID: String? = nil,
        columns: [String] = [],
        where whereClause: String? = nil,
        replica: ElectricReplica = .default,
        headers: [String: String] = [:],
        extraParameters: [String: String] = [:],
        batchApplier: ElectricShapeBatchApplyClosure? = nil
    ) -> ElectricShapeSubscription<Model> {
        let resolvedShapeID = shapeID ?? defaultShapeID(for: table, whereClause: whereClause)
        let shape = ShapeStreamOptions(
            url: shapeURL,
            table: table,
            columns: columns,
            whereClause: whereClause,
            params: extraParameters.mapValues { .string($0) },
            replica: replica,
            headers: headers
        )
        let rowDecoder = self.rowDecoder
        let debugLogger = self.debugLogger

        batchAppliers[resolvedShapeID] = batchApplier ?? { batch, shapeID, context in
            try ElectricSwiftDataRowApplier(
                identifier: identifier,
                rowDecoder: rowDecoder,
                debugLogger: debugLogger
            )
            .apply(batch, shapeID: shapeID, in: context)
        }

        return ElectricShapeSubscription(
            store: self,
            shapeID: resolvedShapeID,
            shape: shape
        )
    }

    func start<Model: SwiftDataCollectionModel>(_ subscription: ElectricShapeSubscription<Model>) async {
        let shapeID = subscription.shapeID
        let stream = ensureSession(for: subscription)
        if runningTasks[shapeID] == nil {
            startTask(for: subscription, stream: stream)
        }

        if await stream.phase() == .paused {
            await stream.resume()
            let state = await stream.currentState()
            record(status: .syncing, state: state, shapeID: shapeID)
        }

        statuses[shapeID] = .syncing
        debugLogger.log(
            .info,
            category: "ShapeStore",
            message: "starting shape subscription",
            metadata: ["shapeID": subscription.shapeID, "table": subscription.shape.table ?? ""]
        )
    }

    func stop(shapeID: String) async {
        let stream = sessions[shapeID]
        runningTasks[shapeID]?.cancel()
        runningTasks[shapeID] = nil
        sessions[shapeID] = nil
        await stream?.stop()
        statuses[shapeID] = .idle
        debugLogger.log(.info, category: "ShapeStore", message: "stopped shape subscription", metadata: ["shapeID": shapeID])
    }

    func pause(shapeID: String) async {
        guard let stream = sessions[shapeID] else { return }
        await stream.pause()
        let state = await stream.currentState()
        record(status: .paused, state: state, shapeID: shapeID)
    }

    func resume<Model: SwiftDataCollectionModel>(_ subscription: ElectricShapeSubscription<Model>) async {
        let stream = ensureSession(for: subscription)
        if runningTasks[subscription.shapeID] == nil {
            startTask(for: subscription, stream: stream)
        }
        await stream.resume()
        let state = await stream.currentState()
        record(status: .syncing, state: state, shapeID: subscription.shapeID)
    }

    func refresh<Model: SwiftDataCollectionModel>(_ subscription: ElectricShapeSubscription<Model>) async {
        let stream = ensureSession(for: subscription)
        if runningTasks[subscription.shapeID] == nil {
            startTask(for: subscription, stream: stream)
        }
        guard await stream.phase() != .paused else { return }
        await stream.refresh()
        let state = await stream.currentState()
        record(status: .syncing, state: state, shapeID: subscription.shapeID)
    }

    func status(for shapeID: String) -> ElectricSubscriptionStatus {
        statuses[shapeID] ?? .idle
    }

    func streamState(for shapeID: String) -> ShapeStreamState? {
        streamStates[shapeID]
    }

    func phase(for shapeID: String) -> ElectricShapePhase? {
        streamStates[shapeID]?.phase
    }

    func checkpoint(for shapeID: String) -> ElectricShapeCheckpoint? {
        streamStates[shapeID]?.checkpoint
    }

    private func defaultShapeID(for table: String, whereClause: String?) -> String {
        if let whereClause, whereClause.isEmpty == false {
            return "\(table):\(whereClause)"
        }
        return table
    }

    private func record(
        status: ElectricSubscriptionStatus,
        state: ShapeStreamState?,
        shapeID: String
    ) {
        statuses[shapeID] = status
        if let state {
            streamStates[shapeID] = state
        }
    }

    private func ensureSession<Model: SwiftDataCollectionModel>(
        for subscription: ElectricShapeSubscription<Model>
    ) -> ShapeStream {
        if let existing = sessions[subscription.shapeID] {
            return existing
        }

        let initialState = loadPersistedState(
            shapeID: subscription.shapeID,
            in: ModelContext(modelContainer)
        ) ?? .init()
        let stream = sessionFactory(subscription.shape, initialState, debugLogger)
        sessions[subscription.shapeID] = stream
        record(status: .syncing, state: initialState, shapeID: subscription.shapeID)
        return stream
    }

    private func startTask<Model: SwiftDataCollectionModel>(
        for subscription: ElectricShapeSubscription<Model>,
        stream: ShapeStream
    ) {
        let shapeID = subscription.shapeID
        let modelContainer = self.modelContainer
        let debugLogger = self.debugLogger
        let observer = self.observer
        let batchApplier = self.batchAppliers[shapeID]

        runningTasks[shapeID] = Task {
            do {
                for try await batch in await stream.batches() {
                    let context = ModelContext(modelContainer)
                    guard let batchApplier else {
                        throw ElectricShapeStoreError.missingBatchApplier(shapeID: shapeID)
                    }
                    debugLogger.log(
                        .debug,
                        category: "ShapeBatch",
                        message: "applying batch to SwiftData",
                        metadata: [
                            "shapeID": shapeID,
                            "messages": String(batch.messages.count),
                            "boundary": batch.boundaryKind.rawValue,
                            "phase": String(describing: batch.phase),
                            "offset": batch.state.offset,
                        ]
                    )
                    let result = try batchApplier(batch, shapeID, context)
                    debugLogger.log(
                        .info,
                        category: "ShapeBatch",
                        message: "applied batch to SwiftData",
                        metadata: [
                            "shapeID": shapeID,
                            "messages": String(batch.messages.count),
                            "observedTXIDs": result.observedTXIDs.map(String.init).joined(separator: ","),
                            "resolvedTransactions": String(result.resolvedTransactionIDs.count),
                            "offset": batch.state.offset
                        ]
                    )
                    if let observer {
                        await observer.shapeStoreDidApply(
                            batch: batch,
                            shapeID: shapeID,
                            resolvedTransactionIDs: result.resolvedTransactionIDs
                        )
                    }
                    self.record(status: .upToDate, state: batch.state, shapeID: shapeID)
                }
            } catch is CancellationError {
                debugLogger.log(.info, category: "ShapeStore", message: "shape subscription cancelled", metadata: ["shapeID": shapeID])
                await self.handleTaskCancellation(shapeID: shapeID, stream: stream)
            } catch {
                debugLogger.log(
                    .error,
                    category: "ShapeStore",
                    message: "shape subscription failed",
                    metadata: ["shapeID": shapeID, "error": String(describing: error)]
                )
                let state = await stream.currentState()
                self.record(
                    status: .error(String(describing: error)),
                    state: state,
                    shapeID: shapeID
                )
                self.clearTask(shapeID: shapeID)
            }
        }
    }

    private func handleTaskCancellation(shapeID: String, stream: ShapeStream) async {
        let state = await stream.currentState()
        if state.phase == .paused {
            record(status: .paused, state: state, shapeID: shapeID)
            return
        }
        record(status: .idle, state: state, shapeID: shapeID)
        clearTask(shapeID: shapeID)
    }

    private func clearTask(shapeID: String) {
        runningTasks[shapeID] = nil
    }

    private func loadPersistedState(
        shapeID: String,
        in context: ModelContext
    ) -> ShapeStreamState? {
        let descriptor = FetchDescriptor<ElectricShapeMetadata>()
        guard let metadata = try? context.fetch(descriptor).first(where: { $0.shapeID == shapeID }) else {
            return nil
        }
        return ShapeStreamState(
            checkpoint: metadata.checkpoint(),
            phase: .initial,
            isUpToDate: false,
            schema: [:]
        )
    }
}

public enum ElectricShapeStoreError: Error, Sendable {
    case missingBatchApplier(shapeID: String)
}
