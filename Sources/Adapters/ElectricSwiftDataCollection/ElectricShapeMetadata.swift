import Foundation
import SwiftData

@Model
public final class ElectricShapeMetadata {
    @Attribute(.unique) public var shapeID: String
    public var handle: String?
    public var offset: String
    public var cursor: String?
    public var lastSyncedAt: Date?

    public init(
        shapeID: String,
        handle: String? = nil,
        offset: String = "-1",
        cursor: String? = nil,
        lastSyncedAt: Date? = nil
    ) {
        self.shapeID = shapeID
        self.handle = handle
        self.offset = offset
        self.cursor = cursor
        self.lastSyncedAt = lastSyncedAt
    }

    public func apply(checkpoint: ElectricShapeCheckpoint) {
        self.handle = checkpoint.handle
        self.offset = checkpoint.offset
        self.cursor = checkpoint.cursor
        self.lastSyncedAt = checkpoint.lastSyncedAt
    }

    public func checkpoint() -> ElectricShapeCheckpoint {
        ElectricShapeCheckpoint(
            handle: handle,
            offset: offset,
            cursor: cursor,
            lastSyncedAt: lastSyncedAt
        )
    }

    @Transient
    public var isUpToDate: Bool {
        lastSyncedAt != nil && offset != "-1"
    }

    @Transient
    public var isLive: Bool {
        false
    }
}
