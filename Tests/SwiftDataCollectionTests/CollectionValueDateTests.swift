@testable import ElectricSwift
@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection
import Foundation
import SwiftData
import Testing

@Suite("CollectionValue Dates")
struct CollectionValueDateTests {
    @Test("Date values round trip through row encoding")
    func dateValuesRoundTripThroughRowEncoding() throws {
        let date = Date(timeIntervalSince1970: 1_781_568_000.603)
        let row: CollectionRow = [
            "id": .string("event-1"),
            "startTime": .date(date),
        ]

        let data = try JSONEncoder().encode(row)
        let decoded = try JSONDecoder().decode(CollectionRow.self, from: data)

        #expect(decoded["id"] == .string("event-1"))
        #expect(decoded["startTime"]?.dateValue == date)
    }

    @Test("Row decoder projects date values into model Date fields")
    func rowDecoderProjectsDateValuesIntoModelDateFields() throws {
        let date = Date(timeIntervalSince1970: 1_781_568_000)
        let row: CollectionRow = [
            "id": .string("event-1"),
            "title": .string("Launch"),
            "startTime": .date(date),
            "completedAt": .null,
        ]

        let value = try CollectionRowDecoder().decode(TestEventValueProjection.self, from: row)

        #expect(value.startTime == date)
        #expect(value.completedAt == nil)
    }

    @Test("Date row helpers are strict")
    func dateRowHelpersAreStrict() throws {
        let date = Date(timeIntervalSince1970: 1_781_568_000)
        let row: CollectionRow = [
            "required": .date(date),
            "nullable": .null,
            "string": .string("2026-06-16T00:00:00Z"),
        ]

        #expect(try row.requiredDate("required") == date)
        #expect(try row.optionalDate("nullable") == nil)
        #expect(try row.optionalDate("missing") == nil)
        #expect(throws: CollectionRowValueError.self) {
            try row.requiredDate("string")
        }
    }

    @Test("Electric conversion uses schema for temporal columns only")
    func electricConversionUsesSchemaForTemporalColumnsOnly() throws {
        let row: ElectricRow = [
            "startTime": .string("2026-06-14 09:18:23.603464+00"),
            "title": .string("2026-06-14 09:18:23.603464+00"),
        ]
        let collectionRow = CollectionRow(
            electricRow: row,
            schema: [
                "startTime": ElectricColumnDefinition(type: "timestamptz"),
                "title": ElectricColumnDefinition(type: "text"),
            ]
        )

        #expect(collectionRow["startTime"]?.dateValue != nil)
        #expect(collectionRow["title"] == .string("2026-06-14 09:18:23.603464+00"))
    }

    @Test("Remote Electric date patch merges with local date row")
    func remoteElectricDatePatchMergesWithLocalDateRow() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let localDate = Date(timeIntervalSince1970: 1_781_568_000)
        let event = TestEvent(
            id: "event-1",
            title: "Local",
            startTime: localDate
        )
        context.insert(event)
        try context.save()

        let applier = ElectricSwiftDataRowApplier(identifier: testEventIdentifier)
        _ = try applier.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"events\"/event-1",
                        value: [
                            "startTime": .string("2026-06-16 00:00:00+00"),
                        ],
                        headers: .init(operation: .update)
                    ),
                ],
                state: testShapeState(offset: "1_0"),
                schema: [
                    "startTime": ElectricColumnDefinition(type: "timestamptz"),
                ],
                reachedUpToDate: false
            ),
            shapeID: "events",
            in: context
        )

        let updated = try #require(context.fetch(testEventIdentifier.fetchDescriptor(for: "event-1")).first)
        let expectedDate = try CollectionRow(electricRow: [
            "startTime": .string("2026-06-16 00:00:00+00"),
        ], schema: [
            "startTime": ElectricColumnDefinition(type: "timestamptz"),
        ]).requiredDate("startTime")
        #expect(updated.title == "Local")
        #expect(updated.startTime == expectedDate)
    }

    @Test("Partial update preserves local date when remote patch omits it")
    func partialUpdatePreservesLocalDateWhenRemotePatchOmitsIt() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let localDate = Date(timeIntervalSince1970: 1_781_568_000)
        context.insert(TestEvent(id: "event-1", title: "Local", startTime: localDate))
        try context.save()

        let applier = ElectricSwiftDataRowApplier(identifier: testEventIdentifier)
        _ = try applier.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"events\"/event-1",
                        value: [
                            "title": .string("Remote"),
                        ],
                        headers: .init(operation: .update)
                    ),
                ],
                state: testShapeState(offset: "2_0"),
                schema: [:],
                reachedUpToDate: false
            ),
            shapeID: "events",
            in: context
        )

        let updated = try #require(context.fetch(testEventIdentifier.fetchDescriptor(for: "event-1")).first)
        #expect(updated.title == "Remote")
        #expect(updated.startTime == localDate)
    }

    @Test("Nullable Electric date fields remain nil")
    func nullableElectricDateFieldsRemainNil() throws {
        let row = CollectionRow(
            electricRow: [
                "completedAt": .null,
            ],
            schema: [
                "completedAt": ElectricColumnDefinition(type: "timestamptz"),
            ]
        )

        #expect(try row.optionalDate("completedAt") == nil)
    }
}

private struct TestEventValueProjection: Decodable {
    let id: String
    let title: String
    let startTime: Date
    let completedAt: Date?
}
