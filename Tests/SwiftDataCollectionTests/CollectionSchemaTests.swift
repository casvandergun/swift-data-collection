import Foundation
import SwiftData
import Testing
@testable import SwiftDataCollection

@Suite("Collection schema and error naming")
struct CollectionSchemaTests {
    @Test("Schema composition registers all runtime metadata exactly once")
    func composition() throws {
        let types = SwiftDataCollectionSchema.models(including: [
            TestTodo.self, PendingCollectionMutation.self,
        ])
        #expect(types.count == 5)
        let container = try ModelContainer(
            for: Schema(types), configurations: ModelConfiguration(isStoredInMemoryOnly: true)
        )
        try SwiftDataCollectionSchema.validate(container)
    }

    @Test("Old runtime schema requires explicit upgrade")
    func missingBaseFailsExplicitly() throws {
        let container = try ModelContainer(
            for: TestTodo.self, PendingCollectionMutation.self,
            PendingCollectionTransaction.self, CollectionMetadata.self,
            configurations: ModelConfiguration(isStoredInMemoryOnly: true)
        )
        #expect(throws: CollectionSchemaError.missingRuntimeModels(["CollectionAuthoritativeBase"])) {
            try SwiftDataCollectionSchema.validate(container)
        }
    }

    @Test("Error and awaiting statuses use the new persisted spelling")
    func statusSpelling() throws {
        #expect(CollectionSyncState.error.rawValue == "error")
        #expect(CollectionSyncState(rawValue: "syncError") == nil)
        let data = try JSONEncoder().encode(CollectionSyncState.error)
        #expect(try JSONDecoder().decode(CollectionSyncState.self, from: data) == .error)
        #expect(PendingMutationStatus.awaiting.rawValue == "awaiting")
        #expect(PendingMutationStatus(rawValue: "awaitingSync") == nil)
        #expect(PendingTransactionState.awaiting.rawValue == "awaiting")
        #expect(PendingTransactionState(rawValue: "awaitingSync") == nil)
    }

    @Test("Current schema preserves error state and delivery metadata across reopen")
    func currentStoreRoundTrip() throws {
        let location = TestStoreLocation()
        defer { location.cleanup() }
        let groupID = UUID()
        do {
            let container = try location.makeContainer()
            let context = ModelContext(container)
            let row = TestTodo(id: "persisted", projectID: "p", title: "Local")
            row.collectionSyncState = .error
            context.insert(row)
            let transaction = PendingCollectionTransaction(
                id: groupID, collectionID: "roundtrip", shapeID: "roundtrip",
                modelName: "TestTodo", sequenceNumber: 19, status: .conflicted
            )
            transaction.dispatchGroupID = groupID
            transaction.submittedMutationsData = Data("[]".utf8)
            transaction.conflictOccurredAt = Date(timeIntervalSince1970: 42)
            context.insert(transaction)
            let metadata = CollectionMetadata(
                collectionID: "roundtrip", shapeID: "roundtrip",
                modelName: "TestTodo", debugName: "Roundtrip"
            )
            metadata.nextTransactionSequence = 20
            context.insert(metadata)
            try context.save()
        }
        let reopened = try location.makeContainer()
        let context = ModelContext(reopened)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "persisted")).first)
        #expect(row.collectionSyncState == .error)
        let transaction = try #require(context.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(transaction.dispatchGroupID == groupID)
        #expect(transaction.submittedMutationsData == Data("[]".utf8))
        #expect(transaction.conflictOccurredAt == Date(timeIntervalSince1970: 42))
        #expect(try context.fetch(FetchDescriptor<CollectionMetadata>()).first?.nextTransactionSequence == 20)
    }
}
