import ElectricSwift
import SwiftDataCollection

public struct ElectricShapeSubscription<Model: SwiftDataCollectionModel>: Sendable {
    let store: ElectricShapeStore

    public let shapeID: String
    public let shape: ShapeStreamOptions

    init(
        store: ElectricShapeStore,
        shapeID: String,
        shape: ShapeStreamOptions
    ) {
        self.store = store
        self.shapeID = shapeID
        self.shape = shape
    }

    public func start() async {
        await store.start(self)
    }

    public func stop() async {
        await store.stop(shapeID: shapeID)
    }

    public func pause() async {
        await store.pause(shapeID: shapeID)
    }

    public func resume() async {
        await store.resume(self)
    }

    public func refresh() async {
        await store.refresh(self)
    }

    public var status: ElectricSubscriptionStatus {
        get async {
            await store.status(for: shapeID)
        }
    }

    public var phase: ElectricShapePhase? {
        get async {
            await store.phase(for: shapeID)
        }
    }

    public var checkpoint: ElectricShapeCheckpoint? {
        get async {
            await store.checkpoint(for: shapeID)
        }
    }

    public var streamState: ShapeStreamState? {
        get async {
            await store.streamState(for: shapeID)
        }
    }
}
