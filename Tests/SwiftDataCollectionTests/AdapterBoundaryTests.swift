@testable import ElectricSwift
@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection
import Foundation
import Testing

@Suite("Adapter Boundaries")
struct AdapterBoundaryTests {
    @Test("SwiftDataCollection core does not import or expose Electric symbols")
    func coreTargetDoesNotImportElectric() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let core = root.appendingPathComponent("Sources/SwiftDataCollection")
        let files = try FileManager.default.contentsOfDirectory(
            at: core,
            includingPropertiesForKeys: nil
        )
        .filter { $0.pathExtension == "swift" }

        for file in files {
            let source = try String(contentsOf: file, encoding: .utf8)
            #expect(source.contains("import ElectricSwift") == false)
            #expect(source.contains("import ElectricSwiftData") == false)
            #expect(source.contains("ElectricRow") == false)
            #expect(source.contains("ElectricValue") == false)
            #expect(source.contains("ElectricModelIdentifier") == false)
        }
    }

    @Test("Electric adapter converts rows both directions")
    func electricAdapterConvertsRowsBothDirections() {
        let electricRow: ElectricRow = [
            "id": .string("todo-1"),
            "done": .boolean(false),
            "count": .integer(2),
            "tags": .array([.string("a"), .string("b")]),
        ]

        let collectionRow = CollectionRow(electricRow: electricRow)
        #expect(collectionRow["id"] == .string("todo-1"))
        #expect(collectionRow["done"] == .boolean(false))
        #expect(collectionRow["count"] == .integer(2))
        #expect(collectionRow["tags"] == .array([.string("a"), .string("b")]))

        #expect(ElectricRow(collectionRow: collectionRow) == electricRow)
    }

    @Test("awaitMatch resolves after an applied Electric batch")
    func awaitMatchResolvesAfterAppliedBatch() async throws {
        let utilities = ElectricCollectionSyncUtilities()
        async let wait: Void = utilities.awaitMatch { message in
            message.headers.operation == .insert &&
                message.value?["title"] == .string("Inserted")
        }
        try await Task.sleep(nanoseconds: 1_000_000)

        let batch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Inserted"),
                    operation: .insert
                ),
            ],
            reachedUpToDate: true
        )

        await utilities.observeAppliedBatch(batch)
        try await wait
    }

    @Test("awaitTxID resolves after an applied Electric batch")
    func awaitTxIDResolvesAfterAppliedBatch() async throws {
        let utilities = ElectricCollectionSyncUtilities()
        async let wait: Void = utilities.awaitTxID(42)
        try await Task.sleep(nanoseconds: 1_000_000)

        let batch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Inserted"),
                    operation: .insert,
                    txids: [42]
                ),
            ],
            reachedUpToDate: true
        )

        await utilities.observeAppliedBatch(batch)
        try await wait
    }

    @Test("awaitMatch times out when no matching Electric batch arrives")
    func awaitMatchTimesOut() async throws {
        let utilities = ElectricCollectionSyncUtilities()

        do {
            try await utilities.awaitMatch(timeout: .milliseconds(10)) { _ in false }
            Issue.record("Expected awaitMatch to time out")
        } catch ElectricCollectionSyncUtilityError.timeout {
            #expect(Bool(true))
        } catch {
            Issue.record("Expected timeout, got \(error)")
        }
    }
}
