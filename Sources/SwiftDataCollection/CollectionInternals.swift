import Foundation
import SwiftData

public enum CollectionLifecycleState: Sendable, Hashable, Codable {
    case idle
    case bootstrapping
    case syncing
    case replaying
    case ready
    case offline
    case error(String)
}

public enum PendingTransactionState: String, Sendable, Hashable, Codable {
    case pending
    case sending
    case awaitingSync
    case resolved
    case failed
    case conflicted
}

@Model
public final class PendingCollectionTransaction {
    @Attribute(.unique) public var id: UUID
    public var collectionID: String
    public var shapeID: String
    public var modelName: String
    public var statusRawValue: String
    public var completionData: Data?
    public var sequenceNumber: Int
    public var attemptCount: Int
    public var createdAt: Date
    public var updatedAt: Date
    public var lastAttemptAt: Date?
    public var nextRetryAt: Date?
    public var lastErrorMessage: String?

    public init(
        id: UUID = UUID(),
        collectionID: String,
        shapeID: String,
        modelName: String,
        sequenceNumber: Int,
        status: PendingTransactionState = .pending,
        completionData: Data? = nil,
        attemptCount: Int = 0,
        createdAt: Date = Date(),
        updatedAt: Date = Date(),
        lastAttemptAt: Date? = nil,
        nextRetryAt: Date? = nil,
        lastErrorMessage: String? = nil
    ) {
        self.id = id
        self.collectionID = collectionID
        self.shapeID = shapeID
        self.modelName = modelName
        self.sequenceNumber = sequenceNumber
        self.statusRawValue = status.rawValue
        self.completionData = completionData
        self.attemptCount = attemptCount
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.lastAttemptAt = lastAttemptAt
        self.nextRetryAt = nextRetryAt
        self.lastErrorMessage = lastErrorMessage
    }

    public var status: PendingTransactionState {
        get { PendingTransactionState(rawValue: statusRawValue) ?? .pending }
        set {
            statusRawValue = newValue.rawValue
            updatedAt = Date()
        }
    }

    func completion() -> CollectionMutationCompletion? {
        guard let completionData else { return nil }
        return try? JSONDecoder().decode(CollectionMutationCompletion.self, from: completionData)
    }

    func setCompletion(_ completion: CollectionMutationCompletion?) {
        completionData = try? completion.map { try JSONEncoder().encode($0) }
        updatedAt = Date()
    }

    public var awaitedObservationTokens: [String] {
        get {
            guard let completion = completion() else { return [] }
            guard case .awaitTokens(let tokens) = completion else { return [] }
            return tokens.sorted()
        }
        set {
            if newValue.isEmpty {
                setCompletion(nil)
            } else {
                setCompletion(.awaitTokens(Set(newValue)))
            }
        }
    }

    func recordAttempt(at date: Date = Date()) {
        attemptCount = attemptCount == Int.max ? Int.max : attemptCount + 1
        lastAttemptAt = date
        updatedAt = date
    }

    func markFailed(
        _ error: Error,
        retryPolicy: some PendingMutationRetryDelaying,
        now: Date = Date()
    ) {
        status = .failed
        lastErrorMessage = String(describing: error)
        nextRetryAt = now.addingTimeInterval(retryPolicy.delay(forAttempt: attemptCount))
        updatedAt = now
    }
}

@Model
public final class CollectionMetadata {
    @Attribute(.unique) public var collectionID: String
    public var shapeID: String
    public var modelName: String
    public var debugName: String
    public var statusData: Data
    public var lastErrorMessage: String?
    public var lastReplayAt: Date?
    public var lastSyncedAt: Date?

    public init(
        collectionID: String,
        shapeID: String,
        modelName: String,
        debugName: String,
        status: CollectionLifecycleState = .idle,
        lastErrorMessage: String? = nil,
        lastReplayAt: Date? = nil,
        lastSyncedAt: Date? = nil
    ) {
        self.collectionID = collectionID
        self.shapeID = shapeID
        self.modelName = modelName
        self.debugName = debugName
        self.statusData = (try? JSONEncoder().encode(status)) ?? Data()
        self.lastErrorMessage = lastErrorMessage
        self.lastReplayAt = lastReplayAt
        self.lastSyncedAt = lastSyncedAt
    }

    public var status: CollectionLifecycleState {
        get { (try? JSONDecoder().decode(CollectionLifecycleState.self, from: statusData)) ?? .idle }
        set { statusData = (try? JSONEncoder().encode(newValue)) ?? Data() }
    }
}

public struct CollectionRetryPolicy: PendingMutationRetryDelaying {
    package static let maximumDelay: TimeInterval = 60

    public init() {}

    public func delay(forAttempt attemptCount: Int) -> TimeInterval {
        let clampedAttempt = max(1, attemptCount)
        let base = min(pow(2.0, Double(clampedAttempt - 1)), Self.maximumDelay)
        let jitter = Double(abs(clampedAttempt % 7)) * 0.137
        let delay = base + jitter
        guard delay.isFinite else { return Self.maximumDelay }
        return min(delay, Self.maximumDelay)
    }
}

package typealias CollectionCommitSaver = @Sendable (ModelContext) throws -> Void
package typealias CollectionRetrySleeper = @Sendable (TimeInterval) async -> Void

package func defaultCollectionRetrySleep(_ delay: TimeInterval) async {
    guard delay.isFinite, delay > 0 else { return }
    let boundedDelay = min(delay, CollectionRetryPolicy.maximumDelay)
    let nanoseconds = UInt64(boundedDelay * 1_000_000_000)
    try? await Task.sleep(nanoseconds: nanoseconds)
}

enum CollectionMutationDispatcher {
    static func groups(from mutations: [PendingCollectionMutation]) -> [[PendingCollectionMutation]] {
        let sorted = mutations.sorted { $0.createdAt < $1.createdAt }
        var groups: [[PendingCollectionMutation]] = []
        for mutation in sorted {
            if var last = groups.popLast(), last.first?.operation == mutation.operation {
                last.append(mutation)
                groups.append(last)
            } else {
                groups.append([mutation])
            }
        }
        return groups
    }
}

struct CollectionMutationQueue {
    let context: ModelContext

    init(modelContainer: ModelContainer) {
        self.context = ModelContext(modelContainer)
    }

    func insert(transaction: PendingCollectionTransaction) {
        context.insert(transaction)
    }

    func insert(mutation: PendingCollectionMutation) {
        context.insert(mutation)
    }

    func saveContext() throws {
        try context.save()
    }

    func fetchPendingTransaction(id: UUID, collectionID: String) -> PendingCollectionTransaction? {
        fetchAllPendingTransactions(collectionID: collectionID).first { $0.id == id }
    }

    func fetchAllPendingTransactions(collectionID: String) -> [PendingCollectionTransaction] {
        ((try? context.fetch(FetchDescriptor<PendingCollectionTransaction>())) ?? [])
            .filter { $0.collectionID == collectionID }
            .sorted { lhs, rhs in
                if lhs.sequenceNumber == rhs.sequenceNumber {
                    return lhs.createdAt < rhs.createdAt
                }
                return lhs.sequenceNumber < rhs.sequenceNumber
            }
    }

    func fetchPendingMutations(transactionID: UUID) -> [PendingCollectionMutation] {
        ((try? context.fetch(FetchDescriptor<PendingCollectionMutation>())) ?? [])
            .filter { $0.transactionID == transactionID }
            .sorted { $0.createdAt < $1.createdAt }
    }

    func fetchAllPendingMutations(collectionID: String) -> [PendingCollectionMutation] {
        let transactionIDs = Set(fetchAllPendingTransactions(collectionID: collectionID).map(\.id))
        return ((try? context.fetch(FetchDescriptor<PendingCollectionMutation>())) ?? [])
            .filter { transactionIDs.contains($0.transactionID) }
            .sorted { $0.createdAt < $1.createdAt }
    }

    func eligibleDispatchTransactionIDs(collectionID: String, now: Date) -> [UUID] {
        fetchAllPendingTransactions(collectionID: collectionID)
            .filter { transaction in
                transaction.status == .pending ||
                transaction.status == .failed && (transaction.nextRetryAt == nil || transaction.nextRetryAt! <= now)
            }
            .filter { transaction in
                hasUnresolvedPredecessor(for: transaction, collectionID: collectionID) == false
            }
            .map(\.id)
    }

    func nextRetryAt(collectionID: String, now: Date) -> Date? {
        fetchAllPendingTransactions(collectionID: collectionID)
            .compactMap { transaction -> Date? in
                guard transaction.status == .failed,
                      let nextRetryAt = transaction.nextRetryAt,
                      nextRetryAt > now else {
                    return nil
                }
                return nextRetryAt
            }
            .min()
    }

    func nextTransactionSequenceNumber(collectionID: String) -> Int {
        (fetchAllPendingTransactions(collectionID: collectionID).map(\.sequenceNumber).max() ?? 0) + 1
    }

    func hasUnresolvedPredecessor(
        for transaction: PendingCollectionTransaction,
        collectionID: String
    ) -> Bool {
        let currentKeys = Set(fetchPendingMutations(transactionID: transaction.id).map(\.targetKey))
        guard currentKeys.isEmpty == false else { return false }

        return fetchAllPendingTransactions(collectionID: collectionID)
            .filter { isEarlier($0, than: transaction) }
            .filter(Self.isUnresolved)
            .contains { earlier in
                let earlierKeys = Set(fetchPendingMutations(transactionID: earlier.id).map(\.targetKey))
                return earlierKeys.isDisjoint(with: currentKeys) == false
            }
    }

    func compactableSuccessorTransactions(
        after transaction: PendingCollectionTransaction,
        collectionID: String,
        targetKey: String,
        operation: CollectionMutationOperation,
        now: Date
    ) -> [(transaction: PendingCollectionTransaction, mutation: PendingCollectionMutation)] {
        var compactable: [(transaction: PendingCollectionTransaction, mutation: PendingCollectionMutation)] = []

        for successor in fetchAllPendingTransactions(collectionID: collectionID)
            .filter({ isEarlier(transaction, than: $0) }) {
            guard successor.status == .pending ||
                  successor.status == .failed && (successor.nextRetryAt == nil || successor.nextRetryAt! <= now) else {
                continue
            }

            let mutations = fetchPendingMutations(transactionID: successor.id)
            guard mutations.contains(where: { $0.targetKey == targetKey }) else {
                continue
            }

            guard mutations.count == 1,
                  let mutation = mutations.first,
                  mutation.operation == operation,
                  mutation.status == .pending ||
                  mutation.status == .failed && (mutation.nextRetryAt == nil || mutation.nextRetryAt! <= now) else {
                break
            }
            compactable.append((successor, mutation))
        }

        return compactable
    }

    private static func isUnresolved(_ transaction: PendingCollectionTransaction) -> Bool {
        switch transaction.status {
        case .pending, .sending, .awaitingSync, .failed:
            true
        case .resolved, .conflicted:
            false
        }
    }

    private func isEarlier(
        _ lhs: PendingCollectionTransaction,
        than rhs: PendingCollectionTransaction
    ) -> Bool {
        if lhs.sequenceNumber == rhs.sequenceNumber {
            return lhs.createdAt < rhs.createdAt
        }
        return lhs.sequenceNumber < rhs.sequenceNumber
    }

    func fetchOrCreateCollectionMetadata(
        collectionID: String,
        shapeID: String,
        modelName: String,
        debugName: String
    ) -> CollectionMetadata {
        if let existing = ((try? context.fetch(FetchDescriptor<CollectionMetadata>())) ?? [])
            .first(where: { $0.collectionID == collectionID }) {
            return existing
        }

        let created = CollectionMetadata(
            collectionID: collectionID,
            shapeID: shapeID,
            modelName: modelName,
            debugName: debugName
        )
        context.insert(created)
        return created
    }
}

struct CollectionMutationReconciler {
    let modelContainer: ModelContainer

    func resolveTransactions(
        observedTokens: Set<String>,
        collectionID: String,
        remainingTokensByTransactionID: inout [UUID: Set<String>],
        awaitedTransactionIDsByToken: inout [String: Set<UUID>]
    ) -> [UUID] {
        guard observedTokens.isEmpty == false else { return [] }
        let context = ModelContext(modelContainer)
        var completedTransactionIDs: [UUID] = []

        for token in observedTokens {
            let affectedTransactions = awaitedTransactionIDsByToken[token] ?? []
            for transactionID in affectedTransactions {
                guard var remaining = remainingTokensByTransactionID[transactionID] else { continue }
                remaining.remove(token)
                if remaining.isEmpty {
                    remainingTokensByTransactionID.removeValue(forKey: transactionID)
                    markTransactionResolved(id: transactionID, collectionID: collectionID, in: context)
                    completedTransactionIDs.append(transactionID)
                } else {
                    remainingTokensByTransactionID[transactionID] = remaining
                }
            }

            for transactionID in affectedTransactions {
                awaitedTransactionIDsByToken[token]?.remove(transactionID)
            }
            if awaitedTransactionIDsByToken[token]?.isEmpty == true {
                awaitedTransactionIDsByToken.removeValue(forKey: token)
            }
        }

        try? context.save()
        return completedTransactionIDs
    }

    func resolveTransaction(
        id: UUID,
        collectionID: String
    ) {
        let context = ModelContext(modelContainer)
        markTransactionResolved(id: id, collectionID: collectionID, in: context)
        try? context.save()
    }

    private func markTransactionResolved(id: UUID, collectionID: String, in context: ModelContext) {
        let transactions = ((try? context.fetch(FetchDescriptor<PendingCollectionTransaction>())) ?? [])
            .filter { $0.collectionID == collectionID && $0.id == id }
        for transaction in transactions {
            transaction.status = .resolved
            transaction.lastErrorMessage = nil
        }

        let mutations = ((try? context.fetch(FetchDescriptor<PendingCollectionMutation>())) ?? [])
            .filter { $0.transactionID == id }
        for mutation in mutations {
            mutation.status = .resolved
            mutation.errorMessage = nil
        }
    }

    static func unresolvedMutations(
        modelName: String,
        targetKey: String,
        in context: ModelContext
    ) -> [PendingCollectionMutation] {
        ((try? context.fetch(FetchDescriptor<PendingCollectionMutation>())) ?? [])
            .filter { $0.modelName == modelName && $0.targetKey == targetKey }
            .filter { mutation in
                switch mutation.status {
                case .pending, .sending, .awaitingSync, .failed, .conflicted:
                    return true
                case .resolved:
                    return false
                }
            }
            .sorted { $0.createdAt < $1.createdAt }
    }

    static func refreshModelState<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>(
        for model: Model.Type,
        key: String,
        modelName: String,
        identifier: CollectionModelIdentifier<Model, ID>,
        in context: ModelContext
    ) throws {
        guard let existing = try context.fetch(identifier.fetchDescriptor(forSerializedKey: key)).first else {
            return
        }

        let pending = unresolvedMutations(modelName: modelName, targetKey: key, in: context)
        existing.collectionPendingMutationCount = pending.count
        if pending.isEmpty {
            existing.collectionSyncState = .synced
        } else if pending.contains(where: { $0.status == .failed || $0.status == .conflicted }) {
            existing.collectionSyncState = .syncError
        } else if pending.contains(where: { $0.operation == .delete }) {
            existing.collectionSyncState = .pendingDelete
        } else if pending.contains(where: { $0.operation == .create }) {
            existing.collectionSyncState = .pendingCreate
        } else {
            existing.collectionSyncState = .pendingUpdate
        }
    }
}

enum CollectionMutationMerger {
    static func merge(
        existing: CollectionMutation,
        incoming: CollectionMutation
    ) throws -> CollectionMutation? {
        switch (existing.operation, incoming.operation) {
        case (.create, .update):
            return CollectionMutation(
                operation: .create,
                key: existing.key,
                original: nil,
                modified: incoming.modified,
                changes: existing.changes.merging(incoming.changes) { _, new in new },
                metadata: incoming.metadata.isEmpty ? existing.metadata : incoming.metadata
            )
        case (.create, .delete):
            return nil
        case (.update, .update):
            return CollectionMutation(
                operation: .update,
                key: existing.key,
                original: existing.original,
                modified: incoming.modified,
                changes: existing.changes.merging(incoming.changes) { _, new in new },
                metadata: incoming.metadata.isEmpty ? existing.metadata : incoming.metadata
            )
        case (.update, .delete):
            return CollectionMutation(
                operation: .delete,
                key: existing.key,
                original: existing.original,
                modified: nil,
                changes: [:],
                metadata: incoming.metadata.isEmpty ? existing.metadata : incoming.metadata
            )
        case (.create, .create), (.delete, .delete):
            return incoming
        case (.delete, .create), (.delete, .update):
            throw CollectionError.invalidMutationSequence(existing.operation, incoming.operation, existing.key)
        case (.update, .create):
            throw CollectionError.invalidMutationSequence(existing.operation, incoming.operation, existing.key)
        }
    }
}

struct PreparedCollectionTransaction {
    let collectionID: String
    let shapeID: String
    let modelName: String
    let transactionID: UUID
    let mutations: [CollectionMutation]
    let optimisticChanges: [OptimisticModelChange]
    let touchedKeys: Set<String>

    var isEmpty: Bool {
        mutations.isEmpty
    }

    func persistedMutations() throws -> [PendingCollectionMutation] {
        try mutations.map { mutation in
            let changedFields = Set(mutation.changes.keys)
            let payload = mutation.modified ?? [:]
            return PendingCollectionMutation(
                transactionID: transactionID,
                modelName: modelName,
                shapeID: shapeID,
                targetKey: mutation.key,
                operation: mutation.operation,
                payloadData: try JSONEncoder().encode(payload),
                changedFieldsData: try JSONEncoder().encode(changedFields),
                originalRowData: try mutation.original.map { try JSONEncoder().encode($0) },
                metadataData: try JSONEncoder().encode(mutation.metadata),
                status: .pending
            )
        }
    }
}

enum OptimisticModelChange {
    case create(key: String, row: CollectionRow)
    case update(key: String, row: CollectionRow)
    case delete(key: String)

    var key: String {
        switch self {
        case .create(let key, _), .update(let key, _), .delete(let key):
            return key
        }
    }

    var operation: CollectionMutationOperation {
        switch self {
        case .create:
            return .create
        case .update:
            return .update
        case .delete:
            return .delete
        }
    }
}

public final class CollectionTransactionBuilder<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>: @unchecked Sendable {
    private let context: ModelContext
    private let transactionID: UUID
    private let collectionID: String
    private let shapeID: String
    private let modelName: String
    private let identifier: CollectionModelIdentifier<Model, ID>
    private let rowDecoder: CollectionRowDecoder
    private let tracer: CollectionTracer
    private var mutationsByKey: [String: CollectionMutation] = [:]
    private var mutationOrder: [String] = []

    init(
        modelContainer: ModelContainer,
        transactionID: UUID,
        collectionID: String,
        shapeID: String,
        modelName: String,
        identifier: CollectionModelIdentifier<Model, ID>,
        rowDecoder: CollectionRowDecoder,
        tracer: CollectionTracer
    ) {
        self.context = ModelContext(modelContainer)
        self.transactionID = transactionID
        self.collectionID = collectionID
        self.shapeID = shapeID
        self.modelName = modelName
        self.identifier = identifier
        self.rowDecoder = rowDecoder
        self.tracer = tracer
    }

    public func insert(
        _ build: @escaping @Sendable () throws -> Model,
        metadata: [String: CollectionValue] = [:]
    ) throws {
        let model = try build()
        let key = identifier.key(for: model)
        guard key.isEmpty == false else {
            throw CollectionError.missingStableIdentifier
        }

        let row = try model.collectionRow()
        let mutation = CollectionMutation(
            operation: .create,
            key: key,
            modified: row,
            changes: row,
            metadata: metadata
        )
        try record(mutation)
    }

    public func update(
        _ key: ID,
        metadata: [String: CollectionValue] = [:],
        _ mutate: @escaping @Sendable (Model) throws -> Void
    ) throws {
        let serializedKey = identifier.serialize(key)
        let original: CollectionRow
        let baseRow: CollectionRow

        if let existing = mutationsByKey[serializedKey] {
            switch existing.operation {
            case .create:
                guard let modified = existing.modified else {
                    throw CollectionError.modelNotFound(serializedKey)
                }
                original = modified
                baseRow = modified
            case .update:
                guard let modified = existing.modified ?? existing.original else {
                    throw CollectionError.modelNotFound(serializedKey)
                }
                original = existing.original ?? modified
                baseRow = modified
            case .delete:
                throw CollectionError.invalidMutationSequence(.delete, .update, serializedKey)
            }
        } else {
            guard let model = try context.fetch(identifier.fetchDescriptor(for: key)).first else {
                throw CollectionError.modelNotFound(serializedKey)
            }
            let row = try model.collectionRow()
            original = row
            baseRow = row
        }

        let model = try Model(collectionRow: baseRow, decoder: rowDecoder)
        try mutate(model)
        let modified = try model.collectionRow()
        let changes = Self.rowDiff(from: original, to: modified)
        guard changes.isEmpty == false else { return }

        let mutation = CollectionMutation(
            operation: .update,
            key: serializedKey,
            original: original,
            modified: modified,
            changes: changes,
            metadata: metadata
        )
        try record(mutation)
    }

    public func delete(
        _ key: ID,
        metadata: [String: CollectionValue] = [:]
    ) throws {
        let serializedKey = identifier.serialize(key)
        let original: CollectionRow?
        if let existing = mutationsByKey[serializedKey] {
            switch existing.operation {
            case .create:
                original = nil
            case .update, .delete:
                original = existing.original ?? existing.modified
            }
        } else {
            guard let model = try context.fetch(identifier.fetchDescriptor(for: key)).first else {
                throw CollectionError.modelNotFound(serializedKey)
            }
            original = try model.collectionRow()
        }

        let mutation = CollectionMutation(
            operation: .delete,
            key: serializedKey,
            original: original,
            changes: [:],
            metadata: metadata
        )
        try record(mutation)
    }

    func preparedTransaction() -> PreparedCollectionTransaction {
        let mutations: [CollectionMutation] = mutationOrder.compactMap { key in
            guard let mutation = mutationsByKey[key] else { return nil }
            return mutation
        }
        let optimisticChanges = mutations.compactMap(Self.optimisticChange(from:))
        return PreparedCollectionTransaction(
            collectionID: collectionID,
            shapeID: shapeID,
            modelName: modelName,
            transactionID: transactionID,
            mutations: mutations,
            optimisticChanges: optimisticChanges,
            touchedKeys: Set(mutations.map { $0.key })
        )
    }

    private func record(_ mutation: CollectionMutation) throws {
        if let existing = mutationsByKey[mutation.key] {
            let merged = try CollectionMutationMerger.merge(existing: existing, incoming: mutation)
            let result = merged?.operation.rawValue ?? "dropped"
            trace(
                .mutationMerged,
                key: mutation.key,
                operation: merged?.operation,
                message: "coalesced \(existing.operation.rawValue)+\(mutation.operation.rawValue) -> \(result)",
                metadata: mutationTraceMetadata(mutation)
            )
            if let merged {
                mutationsByKey[mutation.key] = merged
            } else {
                mutationsByKey.removeValue(forKey: mutation.key)
                mutationOrder.removeAll { $0 == mutation.key }
            }
        } else {
            mutationOrder.append(mutation.key)
            mutationsByKey[mutation.key] = mutation
        }
    }

    private static func rowDiff(from original: CollectionRow, to modified: CollectionRow) -> CollectionRow {
        var changes: CollectionRow = [:]
        for (key, value) in modified where original[key] != value {
            changes[key] = value
        }
        return changes
    }

    private static func optimisticChange(
        from mutation: CollectionMutation
    ) -> OptimisticModelChange? {
        switch mutation.operation {
        case .create:
            guard let row = mutation.modified else { return nil }
            return .create(key: mutation.key, row: row)
        case .update:
            guard let row = mutation.modified else { return nil }
            return .update(key: mutation.key, row: row)
        case .delete:
            return .delete(key: mutation.key)
        }
    }

    private func trace(
        _ kind: CollectionTraceEventKind,
        key: String? = nil,
        operation: CollectionMutationOperation? = nil,
        pendingMutationCount: Int? = nil,
        message: String? = nil,
        metadata: [String: String] = [:]
    ) {
        tracer.record(
            CollectionTraceEvent(
                kind: kind,
                collectionID: collectionID,
                shapeID: shapeID,
                modelName: modelName,
                transactionID: transactionID,
                key: key,
                operation: operation,
                pendingMutationCount: pendingMutationCount,
                message: message,
                metadata: metadata
            )
        )
    }

    private func mutationTraceMetadata(_ mutation: CollectionMutation) -> [String: String] {
        var metadata: [String: String] = [
            "changes": debugString(mutation.changes),
            "changedFields": mutation.changes.keys.sorted().joined(separator: ","),
            "metadata": debugString(mutation.metadata),
        ]
        if let original = mutation.original {
            metadata["original"] = debugString(original)
        }
        if let modified = mutation.modified {
            metadata["modified"] = debugString(modified)
        }
        return metadata
    }

    private func debugString<T: Encodable>(_ value: T) -> String {
        guard let data = try? JSONEncoder().encode(value),
              let string = String(data: data, encoding: .utf8) else {
            return String(describing: value)
        }
        return string
    }
}

public enum CollectionError: Error, Sendable {
    case modelNotFound(String)
    case missingMutationHandler(CollectionMutationOperation)
    case missingStableIdentifier
    case missingAwaitedObservationTokens
    case invalidMutationSequence(CollectionMutationOperation, CollectionMutationOperation, String)
}

public struct CollectionNonRetriableError: Error, Sendable, CustomStringConvertible {
    public let message: String

    public init(_ message: String) {
        self.message = message
    }

    public var description: String { message }
}

package struct CollectionManagedSourceDescriptor: Sendable, Hashable {
    package let sourceID: String

    package init(sourceID: String) {
        self.sourceID = sourceID
    }
}

public enum CollectionManagedRegistrationKind: String, Sendable, Hashable {
    case shape
    case collection
}

package final class CollectionManagedShapeFactory: @unchecked Sendable {
    package let make: @Sendable () -> Any

    package init(make: @escaping @Sendable () -> Any) {
        self.make = make
    }
}

package final class CollectionManagedCollectionFactory: @unchecked Sendable {
    package let make: @Sendable () -> Any

    package init(make: @escaping @Sendable () -> Any) {
        self.make = make
    }
}

package struct CollectionManagedModelRegistration: Sendable {
    package let modelName: String
    package let kind: CollectionManagedRegistrationKind
    package let descriptor: CollectionManagedSourceDescriptor
    package let debugName: String?
    package let shapeFactory: CollectionManagedShapeFactory?
    package let collectionFactory: CollectionManagedCollectionFactory?

    package init(
        modelName: String,
        kind: CollectionManagedRegistrationKind,
        descriptor: CollectionManagedSourceDescriptor,
        debugName: String?,
        shapeFactory: CollectionManagedShapeFactory?,
        collectionFactory: CollectionManagedCollectionFactory?
    ) {
        self.modelName = modelName
        self.kind = kind
        self.descriptor = descriptor
        self.debugName = debugName
        self.shapeFactory = shapeFactory
        self.collectionFactory = collectionFactory
    }
}
