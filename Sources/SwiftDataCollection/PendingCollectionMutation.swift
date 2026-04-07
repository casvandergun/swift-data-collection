import Foundation
import SwiftData

public enum CollectionMutationOperation: String, Sendable, Codable, Hashable {
    case create
    case update
    case delete
}

public enum PendingMutationStatus: String, Sendable, Codable, Hashable {
    case pending
    case sending
    case awaitingSync
    case resolved
    case failed
    case conflicted
}

@Model
public final class PendingCollectionMutation {
    @Attribute(.unique) public var id: UUID
    public var transactionID: UUID
    public var modelName: String
    public var shapeID: String
    public var targetKey: String
    public var operationRawValue: String
    public var payloadData: Data
    public var changedFieldsData: Data?
    public var originalRowData: Data?
    public var metadataData: Data?
    public var awaitedObservationTokensData: Data?
    public var statusRawValue: String
    public var attemptCount: Int
    public var createdAt: Date
    public var lastAttemptAt: Date?
    public var nextRetryAt: Date?
    public var errorMessage: String?

    public init(
        id: UUID = UUID(),
        transactionID: UUID,
        modelName: String,
        shapeID: String,
        targetKey: String,
        operation: CollectionMutationOperation,
        payloadData: Data,
        changedFieldsData: Data? = nil,
        originalRowData: Data? = nil,
        metadataData: Data? = nil,
        awaitedObservationTokensData: Data? = nil,
        status: PendingMutationStatus = .pending,
        attemptCount: Int = 0,
        createdAt: Date = Date(),
        lastAttemptAt: Date? = nil,
        nextRetryAt: Date? = nil,
        errorMessage: String? = nil
    ) {
        self.id = id
        self.transactionID = transactionID
        self.modelName = modelName
        self.shapeID = shapeID
        self.targetKey = targetKey
        self.operationRawValue = operation.rawValue
        self.payloadData = payloadData
        self.changedFieldsData = changedFieldsData
        self.originalRowData = originalRowData
        self.metadataData = metadataData
        self.awaitedObservationTokensData = awaitedObservationTokensData
        self.statusRawValue = status.rawValue
        self.attemptCount = attemptCount
        self.createdAt = createdAt
        self.lastAttemptAt = lastAttemptAt
        self.nextRetryAt = nextRetryAt
        self.errorMessage = errorMessage
    }

    public var operation: CollectionMutationOperation {
        get { CollectionMutationOperation(rawValue: operationRawValue) ?? .update }
        set { operationRawValue = newValue.rawValue }
    }

    public var status: PendingMutationStatus {
        get { PendingMutationStatus(rawValue: statusRawValue) ?? .pending }
        set { statusRawValue = newValue.rawValue }
    }

    public var payload: CollectionRow {
        get { (try? JSONDecoder().decode(CollectionRow.self, from: payloadData)) ?? [:] }
        set { payloadData = (try? JSONEncoder().encode(newValue)) ?? Data() }
    }

    public var changedFields: Set<String> {
        get {
            guard let changedFieldsData else { return [] }
            return (try? JSONDecoder().decode(Set<String>.self, from: changedFieldsData)) ?? []
        }
        set {
            changedFieldsData = try? JSONEncoder().encode(newValue)
        }
    }

    public var originalRow: CollectionRow? {
        get {
            guard let originalRowData else { return nil }
            return try? JSONDecoder().decode(CollectionRow.self, from: originalRowData)
        }
        set {
            originalRowData = try? newValue.map { try JSONEncoder().encode($0) }
        }
    }

    public var metadata: [String: CollectionValue] {
        get {
            guard let metadataData else { return [:] }
            return (try? JSONDecoder().decode([String: CollectionValue].self, from: metadataData)) ?? [:]
        }
        set {
            metadataData = try? JSONEncoder().encode(newValue)
        }
    }

    public var awaitedObservationTokens: [String] {
        get {
            guard let awaitedObservationTokensData else { return [] }
            return (try? JSONDecoder().decode([String].self, from: awaitedObservationTokensData)) ?? []
        }
        set {
            awaitedObservationTokensData = try? JSONEncoder().encode(newValue)
        }
    }

    public func matchesAny(tokens: [String]) -> Bool {
        let awaited = Set(awaitedObservationTokens)
        guard awaited.isEmpty == false else { return false }
        return tokens.contains { awaited.contains($0) }
    }

    public func recordAttempt(at date: Date = Date()) {
        attemptCount += 1
        lastAttemptAt = date
    }

    public func markFailed(_ error: Error, now: Date = Date()) {
        status = .failed
        errorMessage = String(describing: error)
        nextRetryAt = now.addingTimeInterval(Self.retryDelay(forAttempt: attemptCount))
    }

    public func markFailed(
        _ error: Error,
        retryPolicy: some PendingMutationRetryDelaying,
        now: Date = Date()
    ) {
        status = .failed
        errorMessage = String(describing: error)
        nextRetryAt = now.addingTimeInterval(retryPolicy.delay(forAttempt: attemptCount))
    }

    private static func retryDelay(forAttempt attemptCount: Int) -> TimeInterval {
        let clampedAttempt = max(1, attemptCount)
        let base = min(pow(2.0, Double(clampedAttempt - 1)), 60.0)
        let jitter = Double(abs(clampedAttempt % 7)) * 0.137
        return base + jitter
    }
}

public protocol PendingMutationRetryDelaying: Sendable {
    func delay(forAttempt attemptCount: Int) -> TimeInterval
}
