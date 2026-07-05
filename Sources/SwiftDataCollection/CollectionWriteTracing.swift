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

public enum CollectionDiagnosticsLevel: String, Sendable, Codable, Hashable {
    case off
    case summary
    case debug
    case trace

    @available(*, deprecated, renamed: "summary")
    public static let basic: CollectionDiagnosticsLevel = .summary

    @available(*, deprecated, renamed: "debug")
    public static let detailed: CollectionDiagnosticsLevel = .debug

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        let rawValue = try container.decode(String.self)
        switch rawValue {
        case "off":
            self = .off
        case "summary", "basic":
            self = .summary
        case "debug", "detailed":
            self = .debug
        case "trace":
            self = .trace
        default:
            throw DecodingError.dataCorruptedError(
                in: container,
                debugDescription: "Invalid CollectionDiagnosticsLevel value: \(rawValue)"
            )
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
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

    public func filtered(level: CollectionDiagnosticsLevel) -> CollectionDebugLogger {
        CollectionDebugLogger { event in
            guard level.includes(event.level) else { return }
            log(
                event.level,
                category: event.category,
                message: event.message,
                metadata: event.metadata
            )
        }
    }
}

public enum CollectionTraceEventKind: String, Sendable, Codable, Hashable {
    case bootstrapStarted
    case bootstrapCompleted
    case lifecycleChanged
    case replayScheduled
    case replayStarted
    case retryScheduled
    case retryFired
    case connectivityChanged
    case dispatchPausedOffline
    case dispatchResumedOnline
    case awaitedTokensRegistered
    case adapterBatchObserved
    case pendingStateRefreshed
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

public struct CollectionTraceEvent: Sendable, Hashable {
    public let timestamp: Date
    public let kind: CollectionTraceEventKind
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
        kind: CollectionTraceEventKind,
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

    public func filtered(for level: CollectionDiagnosticsLevel) -> CollectionTraceEvent? {
        let filteredMetadata: [String: String]
        switch level {
        case .off:
            return nil
        case .trace:
            filteredMetadata = metadata
        case .debug:
            filteredMetadata = metadata.filter { Self.fullRowPayloadMetadataKeys.contains($0.key) == false }
        case .summary:
            filteredMetadata = metadata.filter { Self.summaryMetadataKeys.contains($0.key) }
        }

        return CollectionTraceEvent(
            timestamp: timestamp,
            kind: kind,
            collectionID: collectionID,
            shapeID: shapeID,
            modelName: modelName,
            transactionID: transactionID,
            key: key,
            operation: operation,
            sequenceNumber: sequenceNumber,
            attemptCount: attemptCount,
            awaitedTokens: awaitedTokens,
            observedTokens: observedTokens,
            resolvedTransactionIDs: resolvedTransactionIDs,
            offset: offset,
            pendingMutationCount: pendingMutationCount,
            message: message,
            errorDescription: errorDescription,
            metadata: filteredMetadata
        )
    }

    private static let summaryMetadataKeys: Set<String> = [
        "attemptCount",
        "awaitedTokens",
        "awaitedTXIDs",
        "awaitingTransactions",
        "changedFields",
        "collectionID",
        "completion",
        "deletedCount",
        "delay",
        "error",
        "expectedRetryAt",
        "connectivity",
        "finalPendingMutationCount",
        "finalSyncState",
        "from",
        "insertedCount",
        "key",
        "keys",
        "messages",
        "modelName",
        "mutationCount",
        "nextRetryAt",
        "observedTokens",
        "observedTXIDs",
        "offset",
        "operation",
        "outcome",
        "pendingMutationCount",
        "protectedFields",
        "reason",
        "resolvedTransactionIDs",
        "sequenceNumber",
        "shapeID",
        "status",
        "to",
        "transactionCount",
        "transactionID",
        "transactionIDs",
        "txids",
        "updatedCount",
    ]

    private static let fullRowPayloadMetadataKeys: Set<String> = [
        "appliedRow",
        "inboundRow",
        "localRowBefore",
    ]

    var level: CollectionDebugLevel {
        switch kind {
        case .transactionFailed:
            .error
        case .transactionCompleted, .shapeBatchApplied:
            .info
        case .handlerReturned, .awaitingSync:
            .info
        case .bootstrapStarted, .bootstrapCompleted, .lifecycleChanged,
             .replayScheduled, .replayStarted, .retryScheduled, .retryFired,
             .connectivityChanged, .dispatchPausedOffline, .dispatchResumedOnline,
             .awaitedTokensRegistered, .adapterBatchObserved, .pendingStateRefreshed:
            .debug
        case .transactionStarted, .optimisticMutationRecorded, .mutationMerged,
             .transactionPersisted, .dispatchEnqueued, .dispatchStarted,
             .handlerInvoked, .mutationResolved:
            .debug
        }
    }

    var category: String {
        "CollectionTrace"
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

public struct CollectionTracer: Sendable {
    private let handler: @Sendable (CollectionTraceEvent) -> Void

    public init(handler: @escaping @Sendable (CollectionTraceEvent) -> Void) {
        self.handler = handler
    }

    public func record(_ event: CollectionTraceEvent) {
        handler(event)
    }

    public static let disabled = CollectionTracer { _ in }

    public func filtered(level: CollectionDiagnosticsLevel) -> CollectionTracer {
        CollectionTracer { event in
            guard let filteredEvent = event.filtered(for: level) else { return }
            record(filteredEvent)
        }
    }

    public static func debugLogger(
        _ logger: CollectionDebugLogger,
        level: CollectionDiagnosticsLevel = .debug
    ) -> CollectionTracer {
        CollectionTracer { event in
            guard let event = event.filtered(for: level) else { return }
            logger.log(
                event.level,
                category: event.category,
                message: event.summary,
                metadata: event.logMetadata
            )
        }
    }

    public static func osLog(
        level: CollectionDiagnosticsLevel = .summary,
        subsystem: String = "SwiftDataCollection",
        category: String = "CollectionTrace"
    ) -> CollectionTracer {
        #if canImport(OSLog)
        let logger = Logger(subsystem: subsystem, category: category)
        #endif

        return CollectionTracer { event in
            guard let event = event.filtered(for: level) else { return }
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

    public static func combining(_ tracers: [CollectionTracer]) -> CollectionTracer {
        CollectionTracer { event in
            for tracer in tracers {
                tracer.record(event)
            }
        }
    }
}

public struct CollectionDiagnostics: Sendable {
    public let logger: CollectionDebugLogger
    public let tracer: CollectionTracer
    public let level: CollectionDiagnosticsLevel

    public init(
        logger: CollectionDebugLogger = .disabled,
        tracer: CollectionTracer = .disabled,
        level: CollectionDiagnosticsLevel = .off
    ) {
        self.logger = logger.filtered(level: level)
        self.tracer = tracer.filtered(level: level)
        self.level = level
    }

    public static let disabled = CollectionDiagnostics()

    public static func logger(
        _ logger: CollectionDebugLogger,
        level: CollectionDiagnosticsLevel = .summary
    ) -> CollectionDiagnostics {
        CollectionDiagnostics(
            logger: logger,
            tracer: CollectionTracer.debugLogger(logger, level: level),
            level: level
        )
    }

    public static func osLog(
        level: CollectionDiagnosticsLevel = .summary,
        subsystem: String = "SwiftDataCollection",
        category: String = "CollectionTrace"
    ) -> CollectionDiagnostics {
        CollectionDiagnostics(
            tracer: CollectionTracer.osLog(
                level: level,
                subsystem: subsystem,
                category: category
            ),
            level: level
        )
    }

    public static func handler(
        level: CollectionDiagnosticsLevel = .debug,
        _ handler: @escaping @Sendable (CollectionTraceEvent) -> Void
    ) -> CollectionDiagnostics {
        CollectionDiagnostics(
            tracer: CollectionTracer(handler: handler),
            level: level
        )
    }
}

private extension CollectionDiagnosticsLevel {
    func includes(_ debugLevel: CollectionDebugLevel) -> Bool {
        switch self {
        case .off:
            return false
        case .summary:
            return debugLevel == .info || debugLevel == .error
        case .debug:
            return debugLevel == .debug || debugLevel == .info || debugLevel == .error
        case .trace:
            return true
        }
    }
}
