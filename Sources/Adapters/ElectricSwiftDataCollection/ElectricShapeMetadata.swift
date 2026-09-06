import Foundation
import SwiftData

@Model
public final class ElectricShapeMetadata {
    @Attribute(.unique) public var shapeID: String
    public var handle: String?
    public var offset: String
    public var cursor: String?
    public var lastSyncedAt: Date?
    /// Persisted so a reset snapshot can span multiple batches and process restarts
    /// without turning a row that appeared in an earlier batch into an absence.
    package var authoritativeSnapshotInProgress: Bool
    package var authoritativeSnapshotSeenKeysData: Data?

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
        self.authoritativeSnapshotInProgress = true
        self.authoritativeSnapshotSeenKeysData = nil
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

    package func beginAuthoritativeSnapshot() {
        authoritativeSnapshotInProgress = true
        authoritativeSnapshotSeenKeysData = nil
    }

    package func recordAuthoritativeSnapshotKey(_ key: String) throws {
        guard authoritativeSnapshotInProgress else { return }
        var keys = try authoritativeSnapshotSeenKeys()
        keys.insert(key)
        authoritativeSnapshotSeenKeysData = try JSONEncoder().encode(keys)
    }

    package func authoritativeSnapshotSeenKeys() throws -> Set<String> {
        guard let authoritativeSnapshotSeenKeysData else { return [] }
        return try JSONDecoder().decode(Set<String>.self, from: authoritativeSnapshotSeenKeysData)
    }

    package func finishAuthoritativeSnapshot() {
        authoritativeSnapshotInProgress = false
        authoritativeSnapshotSeenKeysData = nil
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
