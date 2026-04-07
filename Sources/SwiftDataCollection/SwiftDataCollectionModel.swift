import Foundation
import SwiftData

public enum CollectionValue: Sendable, Hashable, Codable {
    case string(String)
    case integer(Int64)
    case double(Double)
    case boolean(Bool)
    case object([String: CollectionValue])
    case array([CollectionValue])
    case null

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .boolean(value)
        } else if let value = try? container.decode([String: CollectionValue].self) {
            self = .object(value)
        } else if let value = try? container.decode([CollectionValue].self) {
            self = .array(value)
        } else if let value = try? container.decode(Int64.self) {
            self = .integer(value)
        } else if let value = try? container.decode(Double.self) {
            self = .double(value)
        } else {
            self = .string(try container.decode(String.self))
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let value):
            try container.encode(value)
        case .integer(let value):
            try container.encode(value)
        case .double(let value):
            try container.encode(value)
        case .boolean(let value):
            try container.encode(value)
        case .object(let value):
            try container.encode(value)
        case .array(let value):
            try container.encode(value)
        case .null:
            try container.encodeNil()
        }
    }
}

public typealias CollectionRow = [String: CollectionValue]

public struct CollectionRowDecoder: Sendable {
    private let makeDecoder: @Sendable () -> JSONDecoder
    private let makeEncoder: @Sendable () -> JSONEncoder

    public init(
        makeDecoder: @escaping @Sendable () -> JSONDecoder = { JSONDecoder() },
        makeEncoder: @escaping @Sendable () -> JSONEncoder = { JSONEncoder() }
    ) {
        self.makeDecoder = makeDecoder
        self.makeEncoder = makeEncoder
    }

    public func decode<T: Decodable>(_ type: T.Type, from row: CollectionRow) throws -> T {
        let encoder = makeEncoder()
        let data = try encoder.encode(row)
        return try makeDecoder().decode(T.self, from: data)
    }
}

package enum CollectionRowPatcher {
    package static func applying(
        patch: CollectionRow,
        to base: CollectionRow
    ) -> CollectionRow {
        base.merging(patch) { _, incoming in incoming }
    }

    package static func applying(
        patch: CollectionRow,
        to base: CollectionRow,
        preserving fields: Set<String>
    ) -> CollectionRow {
        guard fields.isEmpty == false else {
            return applying(patch: patch, to: base)
        }

        var merged = applying(patch: patch, to: base)
        for field in fields {
            if let original = base[field] {
                merged[field] = original
            }
        }
        return merged
    }
}

public enum CollectionSyncState: String, Sendable, Codable, Hashable {
    case synced
    case pendingCreate
    case pendingUpdate
    case pendingDelete
    case syncError
    case conflicted
}

public protocol SwiftDataCollectionModel: PersistentModel {
    var collectionSyncStateRawValue: String { get set }
    var collectionPendingMutationCount: Int { get set }
    var collectionLastLocalMutationAt: Date? { get set }
    var collectionLastServerVersion: String? { get set }

    init(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws

    func apply(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws
    func collectionRow() throws -> CollectionRow
}

public extension SwiftDataCollectionModel {
    var collectionSyncState: CollectionSyncState {
        get { CollectionSyncState(rawValue: collectionSyncStateRawValue) ?? .synced }
        set { collectionSyncStateRawValue = newValue.rawValue }
    }
}

public struct CollectionModelIdentifier<Model: PersistentModel, ID: Hashable & Sendable>: Sendable {
    public let get: @Sendable (Model) -> ID
    public let makeFetchDescriptor: @Sendable (ID) -> FetchDescriptor<Model>
    public let serialize: @Sendable (ID) -> String
    public let deserialize: @Sendable (String) throws -> ID

    public init(
        get: @escaping @Sendable (Model) -> ID,
        fetchDescriptor: @escaping @Sendable (ID) -> FetchDescriptor<Model>,
        serialize: @escaping @Sendable (ID) -> String,
        deserialize: @escaping @Sendable (String) throws -> ID
    ) {
        self.get = get
        self.makeFetchDescriptor = fetchDescriptor
        self.serialize = serialize
        self.deserialize = deserialize
    }

    public func key(for model: Model) -> String {
        serialize(get(model))
    }

    public func fetchDescriptor(for id: ID) -> FetchDescriptor<Model> {
        makeFetchDescriptor(id)
    }

    public func fetchDescriptor(forSerializedKey key: String) throws -> FetchDescriptor<Model> {
        makeFetchDescriptor(try deserialize(key))
    }
}

public extension CollectionModelIdentifier where ID == String {
    static func string(
        get: @escaping @Sendable (Model) -> String,
        fetchDescriptor: @escaping @Sendable (String) -> FetchDescriptor<Model>
    ) -> Self {
        Self(
            get: get,
            fetchDescriptor: fetchDescriptor,
            serialize: { $0 },
            deserialize: { $0 }
        )
    }
}

public extension CollectionModelIdentifier where ID == UUID {
    static func uuid(
        get: @escaping @Sendable (Model) -> UUID,
        fetchDescriptor: @escaping @Sendable (UUID) -> FetchDescriptor<Model>
    ) -> Self {
        Self(
            get: get,
            fetchDescriptor: fetchDescriptor,
            serialize: \.uuidString,
            deserialize: { value in
                guard let uuid = UUID(uuidString: value) else {
                    throw CollectionModelIdentifierError.invalidSerializedIdentifier(value)
                }
                return uuid
            }
        )
    }
}

public enum CollectionModelIdentifierError: Error, Sendable {
    case invalidSerializedIdentifier(String)
}
