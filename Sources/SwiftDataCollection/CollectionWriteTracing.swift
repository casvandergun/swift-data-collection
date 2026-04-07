import Foundation
#if canImport(OSLog)
import OSLog
#endif

public enum CollectionDebugLevel: String, Sendable, Codable, Hashable {
    case trace
    case debug
    case info
    case error
}

public struct CollectionDebugEvent: Sendable {
    public let level: CollectionDebugLevel
    public let category: String
    public let message: String
    public let metadata: [String: String]

    public init(
        level: CollectionDebugLevel,
        category: String,
        message: String,
        metadata: [String: String] = [:]
    ) {
        self.level = level
        self.category = category
        self.message = message
        self.metadata = metadata
    }
}

public struct CollectionDebugLogger: Sendable {
    private let handler: @Sendable (CollectionDebugEvent) -> Void

    public init(handler: @escaping @Sendable (CollectionDebugEvent) -> Void) {
        self.handler = handler
    }

    public func log(
        _ level: CollectionDebugLevel,
        category: String,
        message: String,
        metadata: [String: String] = [:]
    ) {
        handler(
            CollectionDebugEvent(
                level: level,
                category: category,
                message: message,
                metadata: metadata
            )
        )
    }

    public static let disabled = CollectionDebugLogger { _ in }
}

public enum CollectionWriteDebugEventKind: String, Sendable, Codable, Hashable {
    case transactionStarted
    case optimisticMutationRecorded
    case mutationMerged
    case transactionPersisted
    case dispatchEnqueued
    case dispatchStarted
    case handlerInvoked
    case handlerReturned
    case awaitingSync
    case mutationResolved
    case shapeBatchApplied
    case transactionCompleted
    case transactionFailed
}

public struct CollectionWriteDebugEvent: Sendable, Hashable {
    public let timestamp: Date
    public let kind: CollectionWriteDebugEventKind
    public let collectionID: String
    public let shapeID: String
    public let modelName: String
    public let transactionID: UUID?
    public let key: String?
    public let operation: CollectionMutationOperation?
    public let sequenceNumber: Int?
    public let attemptCount: Int?
    public let awaitedTokens: [String]
    public let observedTokens: [String]
    public let resolvedTransactionIDs: [UUID]
    public let offset: String?
    public let pendingMutationCount: Int?
    public let message: String?
    public let errorDescription: String?
    public let metadata: [String: String]

    public init(
        timestamp: Date = Date(),
        kind: CollectionWriteDebugEventKind,
        collectionID: String,
        shapeID: String,
        modelName: String,
        transactionID: UUID? = nil,
        key: String? = nil,
        operation: CollectionMutationOperation? = nil,
        sequenceNumber: Int? = nil,
        attemptCount: Int? = nil,
        awaitedTokens: [String] = [],
        observedTokens: [String] = [],
        resolvedTransactionIDs: [UUID] = [],
        offset: String? = nil,
        pendingMutationCount: Int? = nil,
        message: String? = nil,
        errorDescription: String? = nil,
        metadata: [String: String] = [:]
    ) {
        self.timestamp = timestamp
        self.kind = kind
        self.collectionID = collectionID
        self.shapeID = shapeID
        self.modelName = modelName
        self.transactionID = transactionID
        self.key = key
        self.operation = operation
        self.sequenceNumber = sequenceNumber
        self.attemptCount = attemptCount
        self.awaitedTokens = awaitedTokens
        self.observedTokens = observedTokens
        self.resolvedTransactionIDs = resolvedTransactionIDs
        self.offset = offset
        self.pendingMutationCount = pendingMutationCount
        self.message = message
        self.errorDescription = errorDescription
        self.metadata = metadata
    }

    var level: CollectionDebugLevel {
        switch kind {
        case .transactionFailed:
            .error
        case .transactionCompleted, .shapeBatchApplied:
            .info
        case .handlerReturned, .awaitingSync:
            .info
        case .transactionStarted, .optimisticMutationRecorded, .mutationMerged,
             .transactionPersisted, .dispatchEnqueued, .dispatchStarted,
             .handlerInvoked, .mutationResolved:
            .debug
        }
    }

    var category: String {
        "CollectionWritePath"
    }

    var summary: String {
        var parts = [kind.rawValue]
        if let transactionID {
            parts.append("tx=\(transactionID.uuidString)")
        }
        if let key {
            parts.append("key=\(key)")
        }
        if let operation {
            parts.append("op=\(operation.rawValue)")
        }
        if let sequenceNumber {
            parts.append("seq=\(sequenceNumber)")
        }
        if let attemptCount {
            parts.append("attempt=\(attemptCount)")
        }
        if awaitedTokens.isEmpty == false {
            parts.append("awaited=\(awaitedTokens)")
        }
        if observedTokens.isEmpty == false {
            parts.append("observed=\(observedTokens)")
        }
        if resolvedTransactionIDs.isEmpty == false {
            let ids = resolvedTransactionIDs.map(\.uuidString)
            parts.append("resolved=\(ids)")
        }
        if let offset {
            parts.append("offset=\(offset)")
        }
        if let pendingMutationCount {
            parts.append("pending=\(pendingMutationCount)")
        }
        if let message, message.isEmpty == false {
            parts.append(message)
        }
        if let errorDescription, errorDescription.isEmpty == false {
            parts.append("error=\(errorDescription)")
        }
        return parts.joined(separator: " ")
    }

    var logMetadata: [String: String] {
        var values = metadata
        values["collectionID"] = collectionID
        values["shapeID"] = shapeID
        values["modelName"] = modelName
        if let transactionID {
            values["transactionID"] = transactionID.uuidString
        }
        if let key {
            values["key"] = key
        }
        if let operation {
            values["operation"] = operation.rawValue
        }
        if let sequenceNumber {
            values["sequenceNumber"] = String(sequenceNumber)
        }
        if let attemptCount {
            values["attemptCount"] = String(attemptCount)
        }
        if awaitedTokens.isEmpty == false {
            values["awaitedTokens"] = awaitedTokens.joined(separator: ",")
        }
        if observedTokens.isEmpty == false {
            values["observedTokens"] = observedTokens.joined(separator: ",")
        }
        if resolvedTransactionIDs.isEmpty == false {
            values["resolvedTransactionIDs"] = resolvedTransactionIDs.map(\.uuidString).joined(separator: ",")
        }
        if let offset {
            values["offset"] = offset
        }
        if let pendingMutationCount {
            values["pendingMutationCount"] = String(pendingMutationCount)
        }
        if let errorDescription {
            values["error"] = errorDescription
        }
        return values
    }
}

public struct CollectionWriteTracer: Sendable {
    private let handler: @Sendable (CollectionWriteDebugEvent) -> Void

    public init(handler: @escaping @Sendable (CollectionWriteDebugEvent) -> Void) {
        self.handler = handler
    }

    public func record(_ event: CollectionWriteDebugEvent) {
        handler(event)
    }

    public static let disabled = CollectionWriteTracer { _ in }

    public static func logger(
        debugLogger: CollectionDebugLogger,
        subsystem: String = "SwiftDataCollection",
        category: String = "WritePath"
    ) -> CollectionWriteTracer {
        #if canImport(OSLog)
        let logger = Logger(subsystem: subsystem, category: category)
        #endif

        return CollectionWriteTracer { event in
            debugLogger.log(
                event.level,
                category: event.category,
                message: event.summary,
                metadata: event.logMetadata
            )

            #if canImport(OSLog)
            switch event.level {
            case .trace, .debug:
                logger.debug("\(event.summary, privacy: .public)")
            case .info:
                logger.info("\(event.summary, privacy: .public)")
            case .error:
                logger.error("\(event.summary, privacy: .public)")
            }
            #endif
        }
    }

    public static func combining(_ tracers: [CollectionWriteTracer]) -> CollectionWriteTracer {
        CollectionWriteTracer { event in
            for tracer in tracers {
                tracer.record(event)
            }
        }
    }
}
