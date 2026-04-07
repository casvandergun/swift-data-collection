@testable import ElectricSwiftDataCollection
@testable import ElectricSwift
import SwiftData
import Testing

@Suite("Electric Collection Protocol Contracts")
struct ElectricCollectionProtocolContractTests {
    @Test("Seeded remote row stays pending until matching echoed update txid arrives")
    func updateRequiresMatchingEchoedTXID() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { _ in ElectricMutationSubmission(awaitedTXIDs: [401]) }
        )

        let context = ModelContext(container)
        let seedBatch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Server original"),
                    operation: .insert,
                    txids: [11]
                ),
            ],
            offset: "1_0",
            reachedUpToDate: true
        )
        let seedResult = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            seedBatch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: seedBatch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: seedResult.resolvedTransactionIDs
        )

        let transaction = try await collection.update("todo-1") { todo in
            todo.title = "Local optimistic title"
        }

        let pendingBefore = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pendingBefore.status == .awaitingSync)

        let optimisticRow = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(optimisticRow.title == "Local optimistic title")
        #expect(optimisticRow.collectionSyncState == .pendingUpdate)

        let unrelatedBatch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Wrong txid title"),
                    operation: .update,
                    txids: [999]
                ),
            ],
            offset: "2_0"
        )
        let unrelatedResult = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            unrelatedBatch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: unrelatedBatch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: unrelatedResult.resolvedTransactionIDs
        )

        let pendingAfterUnrelated = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pendingAfterUnrelated.status == .awaitingSync)
        let stillPendingRow = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(stillPendingRow.title == "Local optimistic title")
        switch await transaction.status {
        case .durablyQueued, .sending, .awaitingSync:
            #expect(Bool(true))
        case .completed, .failed:
            Issue.record("Expected transaction to remain in progress after unrelated txid")
        }

        async let waitForCompletion: Void = transaction.awaitCompletion()

        let matchingBatch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Server authoritative title"),
                    operation: .update,
                    txids: [401]
                ),
            ],
            offset: "3_0"
        )
        let matchingResult = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            matchingBatch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: matchingBatch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: matchingResult.resolvedTransactionIDs
        )

        try await waitForCompletion

        let resolvedMutation = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(resolvedMutation.status == .resolved)

        let authoritativeRow = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(authoritativeRow.title == "Server authoritative title")
        #expect(authoritativeRow.collectionSyncState == .synced)
        #expect(authoritativeRow.collectionPendingMutationCount == 0)
    }

    @Test("Transaction waits for all awaited txids before completing")
    func transactionWaitsForAllAwaitedTXIDs() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [501, 502]) }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }

        let context = ModelContext(container)
        let firstBatch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Inserted"),
                    operation: .insert,
                    txids: [501]
                ),
            ],
            offset: "5_0"
        )
        let firstResult = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            firstBatch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: firstBatch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: firstResult.resolvedTransactionIDs
        )

        switch await transaction.status {
        case .durablyQueued, .sending, .awaitingSync:
            #expect(Bool(true))
        case .completed, .failed:
            Issue.record("Expected transaction to wait for all awaited txids")
        }

        let secondBatch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Inserted"),
                    operation: .update,
                    txids: [502]
                ),
            ],
            offset: "6_0"
        )
        let secondResult = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            secondBatch,
            shapeID: collection.shapeID,
            in: context
        )

        async let waitForCompletion: Void = transaction.awaitCompletion()
        await database.shapeStoreDidApply(
            batch: secondBatch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: secondResult.resolvedTransactionIDs
        )
        try await waitForCompletion

        #expect(await transaction.status == .completed)
    }
}
