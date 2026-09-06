import Foundation
import SwiftData

public enum CollectionValue: Sendable, Hashable, Codable {
    case string(String)
    case integer(Int64)
    case double(Double)
    case boolean(Bool)
    case date(Date)
    case uuid(UUID)
    case object([String: CollectionValue])
    case array([CollectionValue])
    case null

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .boolean(value)
        } else if let value = try? container.decode(CollectionDateValue.self), value.isDateMarker {
            self = .date(Date(timeIntervalSince1970: value.secondsSince1970))
        } else if let value = try? container.decode([String: CollectionValue].self) {
            self = .object(value)
        } else if let value = try? container.decode([CollectionValue].self) {
            self = .array(value)
        } else if let value = try? container.decode(Int64.self) {
            self = .integer(value)
        } else if let value = try? container.decode(Double.self) {
            self = .double(value)
        } else {
            let value = try container.decode(String.self)
            self = UUID(uuidString: value).map(CollectionValue.uuid) ?? .string(value)
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
        case .date(let value):
            try container.encode(CollectionDateValue(date: value))
        case .uuid(let value):
            try container.encode(value.uuidString)
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

public struct CollectionSchema: Sendable, Hashable, Codable {
    public var fields: [String: CollectionFieldType]

    public init(_ fields: [String: CollectionFieldType] = [:]) {
        self.fields = fields
    }
}

public enum CollectionFieldType: Sendable, Hashable, Codable {
    case string
    case integer
    case double
    case boolean
    case date
    case uuid
}

private struct CollectionDateValue: Codable {
    let marker: String
    let secondsSince1970: Double

    var isDateMarker: Bool {
        marker == "date"
    }

    init(date: Date) {
        self.marker = "date"
        self.secondsSince1970 = date.timeIntervalSince1970
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case marker = "__swiftDataCollectionValueType"
        case secondsSince1970
    }

    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        guard Set(container.allKeys) == Set(CodingKeys.allCases) else {
            throw DecodingError.dataCorrupted(
                .init(codingPath: decoder.codingPath, debugDescription: "Not a collection date marker")
            )
        }
        marker = try container.decode(String.self, forKey: .marker)
        secondsSince1970 = try container.decode(Double.self, forKey: .secondsSince1970)
    }
}

public enum CollectionRowValueError: Error, Sendable, Hashable {
    case missingRequiredValue(String)
    case typeMismatch(String, expected: String)
}

public extension CollectionValue {
    static func string(_ value: String?) -> CollectionValue {
        value.map(CollectionValue.string) ?? .null
    }

    static func integer(_ value: Int) -> CollectionValue {
        .integer(Int64(value))
    }

    static func integer(_ value: Int?) -> CollectionValue {
        value.map(CollectionValue.integer) ?? .null
    }

    static func integer(_ value: Int64?) -> CollectionValue {
        value.map(CollectionValue.integer) ?? .null
    }

    static func double(_ value: Double?) -> CollectionValue {
        value.map(CollectionValue.double) ?? .null
    }

    static func double(_ value: Float) -> CollectionValue {
        .double(Double(value))
    }

    static func double(_ value: Float?) -> CollectionValue {
        value.map(CollectionValue.double) ?? .null
    }

    static func boolean(_ value: Bool?) -> CollectionValue {
        value.map(CollectionValue.boolean) ?? .null
    }

    static func date(_ value: Date?) -> CollectionValue {
        value.map(CollectionValue.date) ?? .null
    }

    static func uuid(_ value: UUID?) -> CollectionValue {
        value.map(CollectionValue.uuid) ?? .null
    }

    var dateValue: Date? {
        guard case .date(let value) = self else { return nil }
        return value
    }
}

public extension Dictionary where Key == String, Value == CollectionValue {
    func requiredDate(_ key: String) throws -> Date {
        guard let value = self[key] else {
            throw CollectionRowValueError.missingRequiredValue(key)
        }
        guard case .date(let date) = value else {
            throw CollectionRowValueError.typeMismatch(key, expected: "Date")
        }
        return date
    }

    func optionalDate(_ key: String) throws -> Date? {
        guard let value = self[key] else { return nil }
        switch value {
        case .date(let date):
            return date
        case .null:
            return nil
        default:
            throw CollectionRowValueError.typeMismatch(key, expected: "Date")
        }
    }
}

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
        let data = try encoder.encode(CollectionRowModelProjection(row: row))
        return try makeDecoder().decode(T.self, from: data)
    }
}

private struct CollectionRowModelProjection: Encodable {
    let row: CollectionRow

    func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: DynamicCodingKey.self)
        for (key, value) in row {
            try container.encode(CollectionValueModelProjection(value: value), forKey: DynamicCodingKey(key))
        }
    }
}

private struct CollectionValueModelProjection: Encodable {
    let value: CollectionValue

    func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch value {
        case .string(let value):
            try container.encode(value)
        case .integer(let value):
            try container.encode(value)
        case .double(let value):
            try container.encode(value)
        case .boolean(let value):
            try container.encode(value)
        case .date(let value):
            try container.encode(value)
        case .uuid(let value):
            try container.encode(value)
        case .object(let value):
            try container.encode(CollectionObjectModelProjection(value: value))
        case .array(let value):
            try container.encode(value.map(CollectionValueModelProjection.init(value:)))
        case .null:
            try container.encodeNil()
        }
    }
}

private struct CollectionObjectModelProjection: Encodable {
    let value: [String: CollectionValue]

    func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: DynamicCodingKey.self)
        for (key, value) in value {
            try container.encode(CollectionValueModelProjection(value: value), forKey: DynamicCodingKey(key))
        }
    }
}

private struct DynamicCodingKey: CodingKey {
    let stringValue: String
    let intValue: Int?

    init(_ stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(stringValue: String) {
        self.init(stringValue)
    }

    init?(intValue: Int) {
        self.stringValue = String(intValue)
        self.intValue = intValue
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
    case stagedCreate
    case pendingCreate
    case pendingUpdate
    case pendingDelete
    /// Retryable write failure.
    case error
    case conflicted
}

public protocol SwiftDataCollectionModel: PersistentModel {
    var collectionSyncState: CollectionSyncState { get set }
    var collectionPendingMutationCount: Int { get set }

    init(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws

    func apply(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws
    func collectionRow() throws -> CollectionRow
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
