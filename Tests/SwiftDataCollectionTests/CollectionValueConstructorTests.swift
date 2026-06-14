@testable import ElectricSwift
@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection
import Foundation
import Testing

@Suite("CollectionValue Constructors")
struct CollectionValueConstructorTests {
    @Test("Optional constructors map nil to null")
    func optionalConstructorsMapNilToNull() {
        let string: String? = nil
        let int: Int? = nil
        let int64: Int64? = nil
        let double: Double? = nil
        let float: Float? = nil
        let bool: Bool? = nil
        let date: Date? = nil
        let uuid: UUID? = nil

        #expect(CollectionValue.string(string) == .null)
        #expect(CollectionValue.integer(int) == .null)
        #expect(CollectionValue.integer(int64) == .null)
        #expect(CollectionValue.double(double) == .null)
        #expect(CollectionValue.double(float) == .null)
        #expect(CollectionValue.boolean(bool) == .null)
        #expect(CollectionValue.date(date) == .null)
        #expect(CollectionValue.uuid(uuid) == .null)
    }

    @Test("Optional constructors map values to native collection values")
    func optionalConstructorsMapValuesToNativeCollectionValues() {
        let date = Date(timeIntervalSince1970: 1_781_568_000)
        let uuid = UUID(uuidString: "7E5B2F2A-82EA-4C81-85E0-07C6C68F6FA7")!
        let float = Float(1.25)

        #expect(CollectionValue.string(Optional("value")) == .string("value"))
        #expect(CollectionValue.integer(Optional(12)) == .integer(12))
        #expect(CollectionValue.integer(Optional(Int64(13))) == .integer(13))
        #expect(CollectionValue.double(Optional(2.5)) == .double(2.5))
        #expect(CollectionValue.double(Optional(float)) == .double(Double(float)))
        #expect(CollectionValue.boolean(Optional(true)) == .boolean(true))
        #expect(CollectionValue.date(Optional(date)) == .date(date))
        #expect(CollectionValue.uuid(Optional(uuid)) == .uuid(uuid))
    }

    @Test("Non optional constructors map values to native collection values")
    func nonOptionalConstructorsMapValuesToNativeCollectionValues() {
        let uuid = UUID(uuidString: "5E5CEFAA-37F5-4C77-B27E-8C0B4B3EF929")!
        let float = Float(3.5)

        #expect(CollectionValue.integer(42) == .integer(42))
        #expect(CollectionValue.double(float) == .double(Double(float)))
        #expect(CollectionValue.uuid(uuid) == .uuid(uuid))
        #expect(CollectionValue.string("value") == .string("value"))
        #expect(CollectionValue.integer(Int64(123)) == .integer(123))
        #expect(CollectionValue.date(Date(timeIntervalSince1970: 0)) == .date(Date(timeIntervalSince1970: 0)))
    }

    @Test("UUID values round trip through row encoding")
    func uuidValuesRoundTripThroughRowEncoding() throws {
        let uuid = UUID(uuidString: "E8B8BCE4-9B63-41AF-A9F3-7F4B5817FEAD")!
        let row: CollectionRow = [
            "id": .uuid(uuid),
        ]

        let data = try JSONEncoder().encode(row)
        let decoded = try JSONDecoder().decode(CollectionRow.self, from: data)

        #expect(decoded["id"] == .uuid(uuid))
    }

    @Test("Row decoder projects UUID values into model UUID fields")
    func rowDecoderProjectsUUIDValuesIntoModelUUIDFields() throws {
        let uuid = UUID(uuidString: "4739A14A-2B39-4942-9404-484338911268")!
        let row: CollectionRow = [
            "id": .uuid(uuid),
            "name": .string("Launch"),
        ]

        let value = try CollectionRowDecoder().decode(TestUUIDValueProjection.self, from: row)

        #expect(value.id == uuid)
        #expect(value.name == "Launch")
    }

    @Test("Electric UUID columns normalize into collection UUID values")
    func electricUUIDColumnsNormalizeIntoCollectionUUIDValues() {
        let uuid = UUID(uuidString: "F57C62BD-70A8-4A58-9A34-148758758978")!
        let collectionRow = CollectionRow(
            electricRow: [
                "id": .string(uuid.uuidString),
                "invalid": .string("not-a-uuid"),
                "ids": .array([.string(uuid.uuidString)]),
            ],
            schema: [
                "id": ElectricColumnDefinition(type: "uuid"),
                "invalid": ElectricColumnDefinition(type: "uuid"),
                "ids": ElectricColumnDefinition(type: "uuid", dims: 1),
            ]
        )

        #expect(collectionRow["id"] == CollectionValue.uuid(uuid))
        #expect(collectionRow["invalid"] == CollectionValue.string("not-a-uuid"))
        #expect(collectionRow["ids"] == CollectionValue.array([.uuid(uuid)]))
    }

    @Test("CollectionSchema overrides Electric schema during normalization")
    func collectionSchemaOverridesElectricSchemaDuringNormalization() throws {
        let uuid = UUID(uuidString: "7E7F1F7B-97C1-493E-B95E-F388637019F2")!
        let timestamp = "2026-06-16 00:00:00+00"
        let collectionRow = CollectionRow(
            electricRow: [
                "id": .string(uuid.uuidString),
                "created_at": .string(timestamp),
            ],
            schema: [
                "id": ElectricColumnDefinition(type: "text"),
                "created_at": ElectricColumnDefinition(type: "text"),
            ],
            collectionSchema: CollectionSchema([
                "id": .uuid,
                "created_at": .date,
            ])
        )
        let expectedDate = try CollectionRow(
            electricRow: ["created_at": .string(timestamp)],
            schema: ["created_at": ElectricColumnDefinition(type: "timestamptz")]
        ).requiredDate("created_at")

        #expect(collectionRow["id"] == CollectionValue.uuid(uuid))
        #expect(try collectionRow.requiredDate("created_at") == expectedDate)
    }

    @Test("CollectionSchema invalid parsed values fall back to strings")
    func collectionSchemaInvalidParsedValuesFallBackToStrings() {
        let collectionRow = CollectionRow(
            electricRow: [
                "id": .string("not-a-uuid"),
                "created_at": .string("not-a-date"),
                "deleted_at": .null,
            ],
            schema: [:],
            collectionSchema: CollectionSchema([
                "id": .uuid,
                "created_at": .date,
                "deleted_at": .date,
            ])
        )

        #expect(collectionRow["id"] == CollectionValue.string("not-a-uuid"))
        #expect(collectionRow["created_at"] == CollectionValue.string("not-a-date"))
        #expect(collectionRow["deleted_at"] == .null)
    }

    @Test("CollectionSchema arrays normalize element wise")
    func collectionSchemaArraysNormalizeElementWise() throws {
        let uuid = UUID(uuidString: "3BB17C77-19C3-4AF1-9FE4-C9F93294D034")!
        let timestamp = "2026-06-16 00:00:00+00"
        let collectionRow = CollectionRow(
            electricRow: [
                "ids": .array([.string(uuid.uuidString)]),
                "dates": .array([.string(timestamp)]),
            ],
            schema: [:],
            collectionSchema: CollectionSchema([
                "ids": .uuid,
                "dates": .date,
            ])
        )
        let expectedDate = try CollectionRow(
            electricRow: ["date": .string(timestamp)],
            schema: ["date": ElectricColumnDefinition(type: "timestamptz")]
        ).requiredDate("date")

        #expect(collectionRow["ids"] == CollectionValue.array([.uuid(uuid)]))
        #expect(collectionRow["dates"] == CollectionValue.array([.date(expectedDate)]))
    }

    @Test("Collection UUID values serialize to Electric strings")
    func collectionUUIDValuesSerializeToElectricStrings() {
        let uuid = UUID(uuidString: "94AC7CFB-C5FB-4D67-B2CF-F7CB7D5D99E0")!

        #expect(ElectricValue(collectionValue: .uuid(uuid)) == .string(uuid.uuidString))
    }
}

private struct TestUUIDValueProjection: Decodable {
    let id: UUID
    let name: String
}
