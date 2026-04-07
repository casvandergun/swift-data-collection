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
    private let writeTracer: CollectionWriteTracer
    private let commitSave: CollectionCommitSaver
    private let retryPolicy: any PendingMutationRetryDelaying
    private let retrySleep: CollectionRetrySleeper
    private let foregroundObserverRegistrar: CollectionForegroundObserverRegistrar

    private var bootstrapCompleted = false
    private var foregroundObserversInstalled = false
    private var foregroundObserverTokens: [CollectionForegroundObserverToken] = []
    private var coordinatorsByCollectionID: [String: any CollectionRuntime] = [:]
    private var registrationsByModelName: [String: CollectionManagedModelRegistration] = [:]

    public init(
        modelContainer: ModelContainer,
        rowDecoder: CollectionRowDecoder = .init(),
        debugLogger: CollectionDebugLogger = .disabled,
        writeTracer: CollectionWriteTracer = .disabled
    ) {
        self.init(
            modelContainer: modelContainer,
            rowDecoder: rowDecoder,
            debugLogger: debugLogger,
            writeTracer: writeTracer,
            commitSave: { try $0.save() },
            retryPolicy: CollectionRetryPolicy(),
            retrySleep: defaultCollectionRetrySleep,
            foregroundObserverRegistrar: defaultForegroundObservers
        )
    }

    package init(
        modelContainer: ModelContainer,
        rowDecoder: CollectionRowDecoder = .init(),
        debugLogger: CollectionDebugLogger = .disabled,
        writeTracer: CollectionWriteTracer = .disabled,
        commitSave: @escaping CollectionCommitSaver = { try $0.save() },
        retryPolicy: any PendingMutationRetryDelaying = CollectionRetryPolicy(),
        retrySleep: @escaping CollectionRetrySleeper = defaultCollectionRetrySleep,
        foregroundObserverRegistrar: @escaping CollectionForegroundObserverRegistrar = defaultForegroundObservers
    ) {
        self.modelContainer = modelContainer
        self.rowDecoder = rowDecoder
        self.debugLogger = debugLogger
        self.writeTracer = writeTracer
        self.commitSave = commitSave
        self.retryPolicy = retryPolicy
        self.retrySleep = retrySleep
        self.foregroundObserverRegistrar = foregroundObserverRegistrar
    }

    deinit {
        for token in foregroundObserverTokens {
            token.cancel()
        }
    }

    public func collection<
        Model: SwiftDataCollectionModel,
        ID: Hashable & Sendable
    >(
        _ model: Model.Type,
        options: CollectionOptions<Model, ID>
    ) async throws -> SwiftDataCollection<Model, ID> {
        await bootstrapIfNeeded()

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

            guard options.onInsert == nil, options.onUpdate == nil, options.onDelete == nil else {
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
        coordinatorsByCollectionID[collectionID] = coordinator
        await coordinator.bootstrapIfNeeded()

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

    public func flushPendingMutations() async {
        await bootstrapIfNeeded()
        for coordinator in coordinatorsByCollectionID.values {
            await coordinator.flushPendingMutations()
        }
    }

    package func reportAdapterApplied(
        sourceID: String,
        observedTokens: Set<String>,
        lastSyncedAt: Date?,
        offset: String?
    ) async {
        for coordinator in coordinatorsByCollectionID.values {
            await coordinator.reportAdapterApplied(
                sourceID: sourceID,
                observedTokens: observedTokens,
                lastSyncedAt: lastSyncedAt,
                offset: offset
            )
        }
    }

    private func bootstrapIfNeeded() async {
        guard bootstrapCompleted == false else { return }
        bootstrapCompleted = true
        installForegroundObserversIfNeeded()
    }

    private func installForegroundObserversIfNeeded() {
        guard foregroundObserversInstalled == false else { return }
        foregroundObserversInstalled = true
        foregroundObserverTokens = foregroundObserverRegistrar {
            Task {
                await self.flushPendingMutations()
            }
        }
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
            writeTracer: writeTracer,
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
        let createdCoordinator = CollectionCoordinator<Model, ID>(
            collectionID: collectionID,
            configuration: options,
            sourceID: options.adapter.sourceID,
            adapterRuntime: adapterRuntime,
            modelContainer: modelContainer,
            rowDecoder: rowDecoder,
            debugLogger: debugLogger,
            writeTracer: writeTracer,
            commitSave: commitSave,
            retryPolicy: retryPolicy,
            retrySleep: retrySleep
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
    package var storeWriteTracer: CollectionWriteTracer { writeTracer }

    package func existingRegistration(modelName: String) -> CollectionManagedModelRegistration? {
        registrationsByModelName[modelName]
    }

    package func register(_ registration: CollectionManagedModelRegistration) {
        registrationsByModelName[registration.modelName] = registration
    }

}
