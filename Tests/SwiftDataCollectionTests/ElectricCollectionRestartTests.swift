@testable import ElectricSwiftDataCollection
@testable import ElectricSwift
import SwiftData
import Testing

@Suite("Electric Collection Restart")
struct ElectricCollectionRestartTests {
    actor CallCounter {
        private var count = 0
        private var keys: [String] = []

        func record(key: String) -> Int {
            count += 1
            keys.append(key)
            return count
        }

        func value() -> Int { count }
        func recordedKeys() -> [String] { keys }
    }

    @Test("Bootstrap rebuilds pending insert update and delete state from the durable outbox")
    func bootstrapRebuildsPendingRowState() async throws {
        let storeLocation = TestStoreLocation()
        defer { storeLocation.cleanup() }

        let container = try storeLocation.makeContainer()
        let setupContext = ModelContext(container)
        setupContext.insert(TestTodo(id: "todo-2", projectID: "project-a", title: "Original update"))
        setupContext.insert(TestTodo(id: "todo-3", projectID: "project-a", title: "Original delete"))
        try setupContext.save()

        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) },
            onUpdate: { _ in ElectricMutationSubmission(awaitedTXIDs: [102]) },
            onDelete: { _ in ElectricMutationSubmission(awaitedTXIDs: [103]) }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }
        _ = try await collection.update("todo-2") { todo in
            todo.title = "Updated locally"
        }
        _ = try await collection.delete("todo-3")

        let staleContext = ModelContext(container)
        let rows = try staleContext.fetch(FetchDescriptor<TestTodo>())
        for row in rows {
            row.collectionPendingMutationCount = 0
            row.collectionSyncState = .synced
        }
        try staleContext.save()

        let reopenedContainer = try storeLocation.makeContainer()
        let reopenedDatabase = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: reopenedContainer
        )

        _ = try await reopenedDatabase.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) },
            onUpdate: { _ in ElectricMutationSubmission(awaitedTXIDs: [102]) },
            onDelete: { _ in ElectricMutationSubmission(awaitedTXIDs: [103]) }
        )

        let reopenedContext = ModelContext(reopenedContainer)
        let inserted = try #require(reopenedContext.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(inserted.collectionSyncState == .pendingCreate)
        #expect(inserted.collectionPendingMutationCount == 1)

        let updated = try #require(reopenedContext.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-2")).first)
        #expect(updated.title == "Updated locally")
        #expect(updated.collectionSyncState == .pendingUpdate)
        #expect(updated.collectionPendingMutationCount == 1)

        let deleted = try #require(reopenedContext.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-3")).first)
        #expect(deleted.collectionSyncState == .pendingDelete)
        #expect(deleted.collectionPendingMutationCount == 1)
    }

    @Test("Persisted sending transaction resets and replays after restart")
    func sendingTransactionReplaysAfterRestart() async throws {
        let storeLocation = TestStoreLocation()
        defer { storeLocation.cleanup() }

        let container = try storeLocation.makeContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [111]) }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }

        let sendingContext = ModelContext(container)
        let transaction = try #require(sendingContext.fetch(FetchDescriptor<ElectricPendingTransaction>()).first)
        transaction.status = .sending
        transaction.awaitedTXIDs = []
        transaction.nextRetryAt = nil

        let mutation = try #require(sendingContext.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        mutation.status = .sending
        mutation.awaitedTXIDs = []
        mutation.nextRetryAt = nil
        try sendingContext.save()

        let counter = CallCounter()
        let reopenedContainer = try storeLocation.makeContainer()
        let reopenedDatabase = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: reopenedContainer
        )
        _ = try await reopenedDatabase.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { context in
                _ = await counter.record(key: context.mutations[0].key)
                return ElectricMutationSubmission(awaitedTXIDs: [111])
            }
        )

        try await waitUntil {
            await counter.value() == 1
        }

        let reopenedContext = ModelContext(reopenedContainer)
        let replayed = try #require(reopenedContext.fetch(FetchDescriptor<ElectricPendingTransaction>()).first)
        #expect(replayed.status == .awaitingSync)
    }

    @Test("Persisted awaiting-sync transaction re-registers txids without duplicate dispatch")
    func awaitingSyncTransactionDoesNotRedispatchAfterRestart() async throws {
        let storeLocation = TestStoreLocation()
        defer { storeLocation.cleanup() }

        let container = try storeLocation.makeContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [222]) }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }

        let counter = CallCounter()
        let reopenedContainer = try storeLocation.makeContainer()
        let reopenedDatabase = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: reopenedContainer
        )
        let reopenedCollection = try await reopenedDatabase.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { context in
                _ = await counter.record(key: context.mutations[0].key)
                return ElectricMutationSubmission(awaitedTXIDs: [222])
            }
        )

        try await Task.sleep(nanoseconds: 100_000_000)
        #expect(await counter.value() == 0)

        let reopenedContext = ModelContext(reopenedContainer)
        let awaiting = try #require(reopenedContext.fetch(FetchDescriptor<ElectricPendingTransaction>()).first)
        #expect(awaiting.status == .awaitingSync)

        let batch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Inserted"),
                    operation: .insert,
                    txids: [222]
                ),
            ],
            offset: "8_0"
        )
        let result = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            batch,
            shapeID: reopenedCollection.shapeID,
            in: reopenedContext
        )
        await reopenedDatabase.shapeStoreDidApply(
            batch: batch,
            shapeID: reopenedCollection.shapeID,
            resolvedTransactionIDs: result.resolvedTransactionIDs
        )

        let resolved = try #require(reopenedContext.fetch(FetchDescriptor<ElectricPendingTransaction>()).first)
        #expect(resolved.status == .resolved)
    }

    @Test("Failed optimistic delete stays visible and retryable after restart")
    func failedDeleteRemainsRecoverableAfterRestart() async throws {
        enum SampleError: Error { case sendFailed }

        let storeLocation = TestStoreLocation()
        defer { storeLocation.cleanup() }

        let container = try storeLocation.makeContainer()
        let setupContext = ModelContext(container)
        setupContext.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Existing"))
        try setupContext.save()

        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container,
            retryPolicy: TestRetryPolicy(delayInterval: 3600)
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onDelete: { _ in throw SampleError.sendFailed }
        )

        let transaction = try await collection.delete("todo-1")
        do {
            try await transaction.awaitCompletion()
            Issue.record("Expected delete completion to fail")
        } catch {
            #expect(Bool(true))
        }

        let reopenedContainer = try storeLocation.makeContainer()
        let reopenedDatabase = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: reopenedContainer,
            retryPolicy: TestRetryPolicy(delayInterval: 3600)
        )
        _ = try await reopenedDatabase.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onDelete: { _ in ElectricMutationSubmission(awaitedTXIDs: [301]) }
        )

        let reopenedContext = ModelContext(reopenedContainer)
        let row = try #require(reopenedContext.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.collectionSyncState == .syncError)
        #expect(row.collectionPendingMutationCount == 1)

        let pending = try #require(reopenedContext.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pending.status == .failed)
        #expect(pending.operation == .delete)
    }

    @Test("Due failed transaction resumes automatically after restart")
    func dueFailedTransactionRetriesAfterRestart() async throws {
        enum SampleError: Error { case sendFailed }

        let storeLocation = TestStoreLocation()
        defer { storeLocation.cleanup() }

        let container = try storeLocation.makeContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container,
            retryPolicy: TestRetryPolicy(delayInterval: 3600)
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in throw SampleError.sendFailed }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }
        do {
            try await transaction.awaitCompletion()
            Issue.record("Expected insert completion to fail")
        } catch {
            #expect(Bool(true))
        }

        let failedContext = ModelContext(container)
        let failedTransaction = try #require(failedContext.fetch(FetchDescriptor<ElectricPendingTransaction>()).first)
        failedTransaction.nextRetryAt = Date.distantPast
        let failedMutation = try #require(failedContext.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        failedMutation.nextRetryAt = Date.distantPast
        try failedContext.save()

        let counter = CallCounter()
        let reopenedContainer = try storeLocation.makeContainer()
        let reopenedDatabase = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: reopenedContainer,
            retryPolicy: TestRetryPolicy(delayInterval: 0.01)
        )
        _ = try await reopenedDatabase.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { context in
                _ = await counter.record(key: context.mutations[0].key)
                return ElectricMutationSubmission(awaitedTXIDs: [333])
            }
        )

        try await waitUntil {
            await counter.value() == 1
        }

        let reopenedContext = ModelContext(reopenedContainer)
        let retried = try #require(reopenedContext.fetch(FetchDescriptor<ElectricPendingTransaction>()).first)
        #expect(retried.status == .awaitingSync)
    }

    @Test("Multiple persisted transactions replay in FIFO order after restart")
    func replayPreservesTransactionOrderingAfterRestart() async throws {
        let storeLocation = TestStoreLocation()
        defer { storeLocation.cleanup() }

        let container = try storeLocation.makeContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [1]) }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "First")
        }
        _ = try await collection.insert {
            TestTodo(id: "todo-2", projectID: "project-a", title: "Second")
        }

        let pendingContext = ModelContext(container)
        let transactions = try pendingContext.fetch(FetchDescriptor<ElectricPendingTransaction>())
        for transaction in transactions {
            transaction.status = .pending
            transaction.awaitedTXIDs = []
            transaction.nextRetryAt = nil
        }
        let mutations = try pendingContext.fetch(FetchDescriptor<ElectricPendingMutation>())
        for mutation in mutations {
            mutation.status = .pending
            mutation.awaitedTXIDs = []
            mutation.nextRetryAt = nil
        }
        try pendingContext.save()

        let counter = CallCounter()
        let reopenedContainer = try storeLocation.makeContainer()
        let reopenedDatabase = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: reopenedContainer
        )
        _ = try await reopenedDatabase.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { context in
                _ = await counter.record(key: context.mutations[0].key)
                let txid: Int64 = context.mutations[0].key == "todo-1" ? 401 : 402
                return ElectricMutationSubmission(awaitedTXIDs: [txid])
            }
        )

        try await waitUntil {
            await counter.value() == 2
        }

        #expect(await counter.recordedKeys() == ["todo-1", "todo-2"])
    }

    @Test("Failed atomic commit leaves no partial row or outbox state after restart")
    func failedAtomicCommitLeavesNoStateAfterRestart() async throws {
        enum SampleError: Error { case commitFailed }

        let storeLocation = TestStoreLocation()
        defer { storeLocation.cleanup() }

        let container = try storeLocation.makeContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container,
            commitSave: { _ in throw SampleError.commitFailed }
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) }
        )

        do {
            _ = try await collection.insert {
                TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
            }
            Issue.record("Expected collection insert to fail when atomic commit save fails")
        } catch SampleError.commitFailed {
            #expect(Bool(true))
        } catch {
            Issue.record("Expected commitFailed error, got \(error)")
        }

        let reopenedContainer = try storeLocation.makeContainer()
        let reopenedContext = ModelContext(reopenedContainer)
        #expect(try reopenedContext.fetch(FetchDescriptor<TestTodo>()).isEmpty)
        #expect(try reopenedContext.fetch(FetchDescriptor<ElectricPendingTransaction>()).isEmpty)
        #expect(try reopenedContext.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)
    }

    @Test("Must-refetch deletes only refetchable rows after restart")
    func mustRefetchPreservesPendingRowsAfterRestart() async throws {
        let storeLocation = TestStoreLocation()
        defer { storeLocation.cleanup() }

        let container = try storeLocation.makeContainer()
        let setupContext = ModelContext(container)
        setupContext.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Synced"))
        setupContext.insert(TestTodo(id: "todo-2", projectID: "project-a", title: "Local"))
        try setupContext.save()

        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { _ in ElectricMutationSubmission(awaitedTXIDs: [501]) }
        )

        _ = try await collection.update("todo-2") { todo in
            todo.title = "Pending local update"
        }

        let reopenedContainer = try storeLocation.makeContainer()
        let reopenedDatabase = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: reopenedContainer
        )
        let reopenedCollection = try await reopenedDatabase.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { _ in ElectricMutationSubmission(awaitedTXIDs: [501]) }
        )

        let reopenedContext = ModelContext(reopenedContainer)
        let batch = testTodoBatch(
            messages: [
                testTodoMessage(control: .mustRefetch),
            ],
            offset: "9_0"
        )
        let result = try ElectricCollectionSynchronizer(
            identifier: testTodoIdentifier,
            collectionID: "\(String(reflecting: TestTodo.self)):\(reopenedCollection.shapeID)"
        ).apply(
            batch,
            shapeID: reopenedCollection.shapeID,
            in: reopenedContext
        )
        await reopenedDatabase.shapeStoreDidApply(
            batch: batch,
            shapeID: reopenedCollection.shapeID,
            resolvedTransactionIDs: result.resolvedTransactionIDs
        )

        #expect(try reopenedContext.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).isEmpty)

        let pendingRow = try #require(reopenedContext.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-2")).first)
        #expect(pendingRow.title == "Pending local update")
        #expect(pendingRow.collectionSyncState == .pendingUpdate)
        #expect(pendingRow.collectionPendingMutationCount == 1)
    }
}
