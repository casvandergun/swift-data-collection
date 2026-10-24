import Foundation
import SwiftData
#if canImport(UIKit)
import UIKit
#elseif canImport(AppKit)
import AppKit
#endif

package struct CollectionForegroundObserverToken: @unchecked Sendable {
    let cancel: () -> Void
}

package typealias CollectionForegroundObserverRegistrar =
    (@escaping @Sendable () -> Void) -> [CollectionForegroundObserverToken]

private func defaultForegroundObservers(
    _ handler: @escaping @Sendable () -> Void
) -> [CollectionForegroundObserverToken] {
    var tokens: [CollectionForegroundObserverToken] = []
    let center = NotificationCenter.default
    #if canImport(UIKit)
    let foregroundToken = center.addObserver(
        forName: UIApplication.willEnterForegroundNotification,
        object: nil,
        queue: nil
    ) { _ in
        handler()
    }
    tokens.append(
        CollectionForegroundObserverToken {
            center.removeObserver(foregroundToken)
        }
    )
    #elseif canImport(AppKit)
    let activeToken = center.addObserver(
        forName: NSApplication.didBecomeActiveNotification,
        object: nil,
        queue: nil
    ) { _ in
        handler()
    }
    tokens.append(
        CollectionForegroundObserverToken {
            center.removeObserver(activeToken)
        }
    )
    #endif
    return tokens
}

public actor SwiftDataCollectionStore {
    private let modelContainer: ModelContainer
    private let rowDecoder: CollectionRowDecoder
    private let debugLogger: CollectionDebugLogger
    private let tracer: CollectionTracer
    private let commitSave: CollectionCommitSaver
    private let willSave: CollectionWillSaveHandler?
    private let retryPolicy: any PendingMutationRetryDelaying
    private let retrySleep: CollectionRetrySleeper
    private let foregroundObserverRegistrar: CollectionForegroundObserverRegistrar
    private let connectivityMonitor: any CollectionConnectivityMonitoring

    private var isClosed = false
    private var closeTask: Task<Void, Never>?
    private var bootstrapCompleted = false
    private var foregroundObserversInstalled = false
    private var foregroundObserverTokens: [CollectionForegroundObserverToken] = []
    private let writeGate = CollectionWriteGate()
    private var dispatchLaneStorage: CollectionDispatchLane?
    private var connectivityObserverTask: Task<Void, Never>?
    private var connectivityState: CollectionConnectivityState = .online
    private var coordinatorsByCollectionID: [String: any CollectionRuntime] = [:]
    private var registrationsByModelName: [String: CollectionManagedModelRegistration] = [:]

    /// - Parameter willSave: Called before every save this store makes. See
    ///   `CollectionWillSaveHandler`.
    public init(
        modelContainer: ModelContainer,
        rowDecoder: CollectionRowDecoder = .init(),
        diagnostics: CollectionDiagnostics = .disabled,
        willSave: CollectionWillSaveHandler? = nil
    ) {
        self.init(
            modelContainer: modelContainer,
            rowDecoder: rowDecoder,
            diagnostics: diagnostics,
            commitSave: { try $0.save() },
            willSave: willSave,
            retryPolicy: CollectionRetryPolicy(),
            retrySleep: defaultCollectionRetrySleep,
            foregroundObserverRegistrar: defaultForegroundObservers,
            connectivityMonitor: CollectionNetworkConnectivityMonitor()
        )
    }

    package init(
        modelContainer: ModelContainer,
        rowDecoder: CollectionRowDecoder = .init(),
        diagnostics: CollectionDiagnostics = .disabled,
        commitSave: @escaping CollectionCommitSaver = { try $0.save() },
        willSave: CollectionWillSaveHandler? = nil,
        retryPolicy: any PendingMutationRetryDelaying = CollectionRetryPolicy(),
        retrySleep: @escaping CollectionRetrySleeper = defaultCollectionRetrySleep,
        foregroundObserverRegistrar: @escaping CollectionForegroundObserverRegistrar = defaultForegroundObservers,
        connectivityMonitor: any CollectionConnectivityMonitoring = CollectionNetworkConnectivityMonitor()
    ) {
        self.modelContainer = modelContainer
        self.rowDecoder = rowDecoder
        self.debugLogger = diagnostics.logger
        self.tracer = diagnostics.tracer
        self.commitSave = commitSave
        self.willSave = willSave
        self.retryPolicy = retryPolicy
        self.retrySleep = retrySleep
        self.foregroundObserverRegistrar = foregroundObserverRegistrar
        self.connectivityMonitor = connectivityMonitor
    }

    deinit {
        for token in foregroundObserverTokens {
            token.cancel()
        }
        connectivityObserverTask?.cancel()
    }

    public func collection<
        Model: SwiftDataCollectionModel,
        ID: Hashable & Sendable
    >(
        _ model: Model.Type,
        options: CollectionOptions<Model, ID>
    ) async throws -> SwiftDataCollection<Model, ID> {
        guard !isClosed else { throw SwiftDataCollectionStoreError.closed }
        try SwiftDataCollectionSchema.validate(modelContainer)
        await bootstrapIfNeeded()
        guard !isClosed else { throw SwiftDataCollectionStoreError.closed }

        let descriptor = CollectionManagedSourceDescriptor(sourceID: options.adapter.sourceID)
        if let existing = registrationsByModelName[options.modelName] {
            guard existing.kind == .collection, existing.descriptor == descriptor else {
                throw SwiftDataCollectionStoreError.managedShapeConflict(
                    modelName: options.modelName,
                    existingKind: existing.kind,
                    existingShapeID: existing.descriptor.sourceID,
                    requestedKind: .collection,
                    requestedShapeID: descriptor.sourceID
                )
            }

            guard options.onApply == nil,
                  options.onInsert == nil,
                  options.onUpdate == nil,
                  options.onDelete == nil else {
                throw SwiftDataCollectionStoreError.managedShapeConflict(
                    modelName: options.modelName,
                    existingKind: existing.kind,
                    existingShapeID: existing.descriptor.sourceID,
                    requestedKind: .collection,
                    requestedShapeID: descriptor.sourceID
                )
            }

            guard let existingCollection = existing.collectionFactory?.make() as? SwiftDataCollection<Model, ID> else {
                throw SwiftDataCollectionStoreError.managedShapeConflict(
                    modelName: options.modelName,
                    existingKind: existing.kind,
                    existingShapeID: existing.descriptor.sourceID,
                    requestedKind: .collection,
                    requestedShapeID: descriptor.sourceID
                )
            }
            return existingCollection
        }

        let collectionID = "\(options.modelName):\(options.adapter.sourceID)"
        let coordinator: CollectionCoordinator<Model, ID> = try await makeCoordinator(
            collectionID: collectionID,
            options: options
        )
        guard !isClosed else {
            await coordinator.close()
            throw SwiftDataCollectionStoreError.closed
        }
        coordinatorsByCollectionID[collectionID] = coordinator
        await dispatchLane.register(coordinator, collectionID: collectionID)
        await coordinator.bootstrapIfNeeded()
        guard !isClosed else {
            await coordinator.close()
            throw SwiftDataCollectionStoreError.closed
        }

        let collection = SwiftDataCollection(
            coordinator: coordinator,
            sourceID: options.adapter.sourceID,
            debugName: options.debugName
        )
        registrationsByModelName[options.modelName] = CollectionManagedModelRegistration(
            modelName: options.modelName,
            kind: .collection,
            descriptor: descriptor,
            debugName: options.debugName,
            shapeFactory: nil,
            collectionFactory: CollectionManagedCollectionFactory {
                collection
            }
        )

        return collection
    }

    public func flush() async {
        guard !isClosed else { return }
        await bootstrapIfNeeded()
        guard !isClosed else { return }
        await dispatchLane.drain()
    }

    /// Permanently closes this store's runtime without deleting its durable
    /// rows or outbox. Repeated calls join the same drain. Retire collection
    /// and shape handles afterward; a later session uses a new store.
    public func close() async {
        if let closeTask { await closeTask.value; return }
        guard !isClosed else { return }
        isClosed = true
        for token in foregroundObserverTokens { token.cancel() }
        foregroundObserverTokens.removeAll()
        connectivityMonitor.stop()
        let observing = connectivityObserverTask
        connectivityObserverTask = nil
        observing?.cancel()
        let closing = Task {
            // Closing dispatch first cancels retries and drains only the
            // already executing handler. Queued work stays durable.
            await dispatchLaneStorage?.close()
            for coordinator in orderedCoordinators {
                await coordinator.setConnectivityState(.offline)
                await coordinator.close()
            }
            for registration in registrationsByModelName.values {
                await registration.shapeFactory?.stop()
            }
            await observing?.value
            registrationsByModelName.removeAll()
            coordinatorsByCollectionID.removeAll()
            dispatchLaneStorage = nil
        }
        closeTask = closing
        await closing.value
        closeTask = nil
    }

    package func reportAdapterApplied(
        sourceID: String,
        observedTokens: Set<String>,
        lastSyncedAt: Date?,
        offset: String?
    ) async {
        for coordinator in orderedCoordinators {
            await coordinator.reportAdapterApplied(
                sourceID: sourceID,
                observedTokens: observedTokens,
                lastSyncedAt: lastSyncedAt,
                offset: offset
            )
        }
    }

    /// Coordinators in a stable order.
    ///
    /// Dictionary iteration order varies per process, which made the store's
    /// fan-out loops nondeterministic -- including the reconnect path, where it
    /// decided whether a bug reordered anything on a given run.
    private var orderedCoordinators: [any CollectionRuntime] {
        coordinatorsByCollectionID
            .sorted { $0.key < $1.key }
            .map(\.value)
    }

    private func bootstrapIfNeeded() async {
        guard !isClosed, bootstrapCompleted == false else { return }
        bootstrapCompleted = true
        connectivityMonitor.start()
        connectivityState = connectivityMonitor.currentState()
        installConnectivityObserverIfNeeded()
        installForegroundObserversIfNeeded()
    }

    private func installConnectivityObserverIfNeeded() {
        guard connectivityObserverTask == nil else { return }
        let updates = connectivityMonitor.updates()
        connectivityObserverTask = Task {
            for await state in updates {
                await self.setConnectivityState(state)
            }
        }
    }

    private func setConnectivityState(_ state: CollectionConnectivityState) async {
        guard !isClosed, connectivityState != state else { return }
        connectivityState = state
        /*
         * The lane is opened last and closed first, because a collection that
         * is still offline answers the lane with `skipped` -- and the lane
         * steps past a skip. Opening the lane before every coordinator knows it
         * is online therefore lets a child dispatch while its parent's
         * collection is still refusing, which is the ordering bug this design
         * exists to prevent.
         */
        switch state {
        case .offline:
            await dispatchLane.setConnectivityState(state)
            for coordinator in orderedCoordinators {
                await coordinator.setConnectivityState(state)
            }
        case .online:
            for coordinator in orderedCoordinators {
                await coordinator.setConnectivityState(state)
            }
            await dispatchLane.setConnectivityState(state)
            await flush()
        }
    }

    private func installForegroundObserversIfNeeded() {
        guard foregroundObserversInstalled == false else { return }
        foregroundObserversInstalled = true
        foregroundObserverTokens = foregroundObserverRegistrar {
            Task {
                await self.flush()
            }
        }
    }

    private var dispatchLane: CollectionDispatchLane {
        if let dispatchLaneStorage {
            return dispatchLaneStorage
        }
        let lane = CollectionDispatchLane(
            modelContainer: modelContainer,
            retrySleep: retrySleep,
            tracer: tracer,
            connectivityState: connectivityState
        )
        dispatchLaneStorage = lane
        return lane
    }

    private func makeCoordinator<
        Model: SwiftDataCollectionModel,
        ID: Hashable & Sendable
    >(
        collectionID: String,
        options: CollectionOptions<Model, ID>
    ) async throws -> CollectionCoordinator<Model, ID> {
        let relay = CollectionAdapterEventRelay<Model, ID>()

        let context = CollectionAdapterContext<Model, ID>(
            modelContainer: modelContainer,
            collectionID: collectionID,
            sourceID: options.adapter.sourceID,
            debugName: options.debugName,
            identifier: options.identifier,
            rowDecoder: rowDecoder,
            debugLogger: debugLogger,
            tracer: tracer,
            writeGate: writeGate,
            willSave: willSave,
            onApply: options.onApply,
            reportApplied: { observedTokens, lastSyncedAt, offset in
                await relay.reportApplied(
                    observedTokens: observedTokens,
                    lastSyncedAt: lastSyncedAt,
                    offset: offset
                )
            },
            reportRefreshCompleted: { lastSyncedAt in
                await relay.reportRefreshCompleted(lastSyncedAt: lastSyncedAt)
            },
            reportError: { error in
                await relay.reportError(error)
            }
        )

        let adapterRuntime = try await options.adapter.makeRuntime(context)
        guard !isClosed else {
            await adapterRuntime.stop()
            throw SwiftDataCollectionStoreError.closed
        }
        let createdCoordinator = CollectionCoordinator<Model, ID>(
            collectionID: collectionID,
            configuration: options,
            sourceID: options.adapter.sourceID,
            adapterRuntime: adapterRuntime,
            modelContainer: modelContainer,
            rowDecoder: rowDecoder,
            debugLogger: debugLogger,
            tracer: tracer,
            writeGate: writeGate,
            commitSave: commitSave,
            willSave: willSave,
            retryPolicy: retryPolicy,
            dispatchLane: dispatchLane,
            connectivityState: connectivityState
        )
        await relay.bind(createdCoordinator)
        return createdCoordinator
    }
}

package actor CollectionAdapterEventRelay<
    Model: SwiftDataCollectionModel,
    ID: Hashable & Sendable
> {
    private var coordinator: CollectionCoordinator<Model, ID>?

    func bind(_ coordinator: CollectionCoordinator<Model, ID>) {
        self.coordinator = coordinator
    }

    func reportApplied(
        observedTokens: Set<String>,
        lastSyncedAt: Date?,
        offset: String?
    ) async {
        await coordinator?.didApplyFromAdapter(
            observedTokens: observedTokens,
            lastSyncedAt: lastSyncedAt,
            offset: offset
        )
    }

    func reportRefreshCompleted(lastSyncedAt: Date?) async {
        await coordinator?.didRefreshComplete(lastSyncedAt: lastSyncedAt)
    }

    func reportError(_ error: Error) async {
        await coordinator?.didEncounterAdapterError(error)
    }
}

public enum SwiftDataCollectionStoreError: Error, Sendable {
    case closed
    case managedShapeConflict(
        modelName: String,
        existingKind: CollectionManagedRegistrationKind,
        existingShapeID: String,
        requestedKind: CollectionManagedRegistrationKind,
        requestedShapeID: String
    )
}

extension SwiftDataCollectionStore {
    package var storeModelContainer: ModelContainer { modelContainer }
    package var storeRowDecoder: CollectionRowDecoder { rowDecoder }
    package var storeDebugLogger: CollectionDebugLogger { debugLogger }
    package var storeTracer: CollectionTracer { tracer }
    package var storeWriteGate: CollectionWriteGate { writeGate }
    package var storeWillSave: CollectionWillSaveHandler? { willSave }

    package func existingRegistration(modelName: String) -> CollectionManagedModelRegistration? {
        registrationsByModelName[modelName]
    }

    package var storeIsClosed: Bool { isClosed }

    package func register(_ registration: CollectionManagedModelRegistration) throws {
        guard !isClosed else { throw SwiftDataCollectionStoreError.closed }
        registrationsByModelName[registration.modelName] = registration
    }

}
