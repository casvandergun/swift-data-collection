import Foundation

/// Whether a parked conflict can be safely abandoned using its retained base.
public enum CollectionConflictRepairReadiness: String, Sendable, Hashable, Codable {
    case ready
    case requiresAuthoritativeRecovery
    case unsupportedLegacySubmission
}

/// One key and operation represented by a permanently refused dispatch group.
public struct CollectionConflictEntry: Sendable, Hashable, Codable {
    public let key: String
    public let operation: CollectionMutationOperation
    public let localChanges: CollectionRow
    public let baselineEvidence: CollectionBaselineEvidence

    public init(
        key: String,
        operation: CollectionMutationOperation,
        localChanges: CollectionRow,
        baselineEvidence: CollectionBaselineEvidence
    ) {
        self.key = key
        self.operation = operation
        self.localChanges = localChanges
        self.baselineEvidence = baselineEvidence
    }
}

/// A complete diagnostic snapshot of one permanently refused dispatch group.
public struct CollectionConflict: Sendable, Hashable, Codable, Identifiable {
    public let id: UUID
    public let transactionIDs: [UUID]
    public let error: String
    public let occurredAt: Date
    public let repairReadiness: CollectionConflictRepairReadiness
    public let entries: [CollectionConflictEntry]

    public init(
        id: UUID,
        transactionIDs: [UUID],
        error: String,
        occurredAt: Date,
        repairReadiness: CollectionConflictRepairReadiness,
        entries: [CollectionConflictEntry]
    ) {
        self.id = id
        self.transactionIDs = transactionIDs
        self.error = error
        self.occurredAt = occurredAt
        self.repairReadiness = repairReadiness
        self.entries = entries
    }

    public var conflictID: UUID { id }
}

public enum CollectionConflictError: Error, Sendable, Hashable {
    case notFound(UUID)
    case notConflicted(UUID)
    case requiresAuthoritativeRecovery(UUID, keys: [String])
    case unsupportedLegacySubmission(UUID)
}
