@testable import ElectricSwiftDataCollection
@testable import ElectricSwift
import SwiftData
import Testing

@Suite("Collection Same-Key Dispatch")
struct CollectionSameKeyDispatchTests {
    actor MutationRecorder {
        struct Call: Sendable, Equatable {
            let operation: ElectricMutationOperation
            let key: String
            let mutationCount: Int
            let title: String?
            let changes: CollectionRow
        }

        private var calls: [Call] = []

        func record(
            operation: ElectricMutationOperation,
            context: CollectionMutationContext<TestTodo, String>
        ) {
            let mutation = context.mutations[0]
            calls.append(
                Call(
                    operation: operation,
                    key: mutation.key,
                    mutationCount: context.mutations.count,
                    title: mutation.modified?["title"]?.stringValue,
                    changes: mutation.changes
                )
            )
        }

        func recordedCalls() -> [Call] {
            calls
        }
    }

    @Test("Same-key updates wait behind pending create and compact to latest state")
    func sameKeyUpdatesWaitBehindPendingCreateAndCompactToLatestState() async throws {
        let recorder = MutationRecorder()
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { context in
                await recorder.record(operation: .create, context: context)
                return ElectricMutationSubmission(awaitedTXIDs: [101])
            },
            onUpdate: { context in
                await recorder.record(operation: .update, context: context)
                return ElectricMutationSubmission(awaitedTXIDs: [102])
            }
        )

        let createTransaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Draft")
        }
        try await waitUntil {
            await recorder.recordedCalls().count == 1
        }

        _ = try await collection.update("todo-1") { todo in
            todo.title = "Finalizing"
        }
        _ = try await collection.update("todo-1") { todo in
            todo.title = "Finished"
        }
        await database.flush()

        let callsBeforeCreateAck = await recorder.recordedCalls()
        #expect(callsBeforeCreateAck.map(\.operation) == [.create])

        let context = ModelContext(container)
        let createBatch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Draft"),
                    operation: .insert,
                    txids: [101]
                ),
            ],
            offset: "1_0"
        )
        let createResult = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            createBatch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: createBatch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: createResult.resolvedTransactionIDs
        )
        try await createTransaction.wait()

        try await waitUntil {
            await recorder.recordedCalls().filter { $0.operation == .update }.count == 1
        }

        let callsAfterCreateAck = await recorder.recordedCalls()
        #expect(callsAfterCreateAck.map(\.operation) == [.create, .update])
        let updateCall = try #require(callsAfterCreateAck.last)
        #expect(updateCall.key == "todo-1")
        #expect(updateCall.mutationCount == 1)
        #expect(updateCall.title == "Finished")
        #expect(updateCall.changes["title"] == .string("Finished"))
    }

    @Test("No-op update does not persist mutation or invoke handler")
    func noOpUpdateDoesNotPersistMutationOrInvokeHandler() async throws {
        let recorder = MutationRecorder()
        let container = try makeTestContainer()
        let setupContext = ModelContext(container)
        setupContext.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Existing"))
        try setupContext.save()

        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { context in
                await recorder.record(operation: .update, context: context)
                return ElectricMutationSubmission(awaitedTXIDs: [201])
            }
        )

        let transaction = try await collection.update("todo-1") { _ in }
        try await transaction.wait()
        await database.flush()

        #expect(await recorder.recordedCalls().isEmpty)
        let context = ModelContext(container)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingTransaction>()).isEmpty)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)

        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.collectionSyncState == .synced)
        #expect(row.collectionPendingMutationCount == 0)
    }

    @Test("Restart preserves same-key update block behind awaiting create")
    func restartPreservesSameKeyUpdateBlockBehindAwaitingCreate() async throws {
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
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [301]) },
            onUpdate: { _ in ElectricMutationSubmission(awaitedTXIDs: [302]) }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Draft")
        }
        _ = try await collection.update("todo-1") { todo in
            todo.title = "Finished"
        }

        let recorder = MutationRecorder()
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
                await recorder.record(operation: .create, context: context)
                return ElectricMutationSubmission(awaitedTXIDs: [301])
            },
            onUpdate: { context in
                await recorder.record(operation: .update, context: context)
                return ElectricMutationSubmission(awaitedTXIDs: [302])
            }
        )
        await reopenedDatabase.flush()

        #expect(await recorder.recordedCalls().isEmpty)

        let reopenedContext = ModelContext(reopenedContainer)
        let createBatch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(title: "Draft"),
                    operation: .insert,
                    txids: [301]
                ),
            ],
            offset: "2_0"
        )
        let createResult = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            createBatch,
            shapeID: reopenedCollection.shapeID,
            in: reopenedContext
        )
        await reopenedDatabase.shapeStoreDidApply(
            batch: createBatch,
            shapeID: reopenedCollection.shapeID,
            resolvedTransactionIDs: createResult.resolvedTransactionIDs
        )

        try await waitUntil {
            await recorder.recordedCalls().filter { $0.operation == .update }.count == 1
        }

        let updateCall = try #require(await recorder.recordedCalls().last)
        #expect(updateCall.operation == .update)
        #expect(updateCall.key == "todo-1")
        #expect(updateCall.title == "Finished")
    }
}

private extension CollectionValue {
    var stringValue: String? {
        guard case .string(let value) = self else { return nil }
        return value
    }
}
