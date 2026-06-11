@testable import FetchSwiftDataCollection
@testable import SwiftDataCollection
import SwiftData
import Testing

@Suite("Fetch SwiftData Collection")
struct FetchSwiftDataCollectionTests {
    @Test("Start inserts fetched rows")
    func startInsertsFetchedRows() async throws {
        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "One"),
            testTodoCollectionRow(id: "todo-2", title: "Two"),
        ])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)

        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() }
            )
        )

        await collection.start()

        let context = ModelContext(container)
        let fetched = try context.fetch(FetchDescriptor<TestTodo>())
        #expect(fetched.map(\.id).sorted() == ["todo-1", "todo-2"])
    }

    @Test("Refresh updates existing rows and inserts new rows")
    func refreshUpdatesAndInsertsRows() async throws {
        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "Original"),
        ])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() }
            )
        )

        await collection.start()
        await rows.set([
            testTodoCollectionRow(id: "todo-1", title: "Updated"),
            testTodoCollectionRow(id: "todo-2", title: "Inserted"),
        ])
        await collection.refresh()

        let context = ModelContext(container)
        let todo1 = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        let todo2 = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-2")).first)
        #expect(todo1.title == "Updated")
        #expect(todo2.title == "Inserted")
    }

    @Test("Delete synced rows policy removes missing synced rows")
    func deleteSyncedRowsRemovesMissingRows() async throws {
        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "One"),
            testTodoCollectionRow(id: "todo-2", title: "Two"),
        ])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                missingRowPolicy: .deleteSyncedRows,
                fetch: { _ in await rows.all() }
            )
        )

        await collection.start()
        await rows.set([
            testTodoCollectionRow(id: "todo-1", title: "One"),
        ])
        await collection.refresh()

        let context = ModelContext(container)
        #expect(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).count == 1)
        #expect(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-2")).isEmpty)
    }

    @Test("Delete synced rows policy preserves missing rows with unresolved mutations")
    func deleteSyncedRowsPreservesMissingPendingRows() async throws {
        enum SampleError: Error { case rejected }

        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "One"),
            testTodoCollectionRow(id: "todo-2", title: "Two"),
        ])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                missingRowPolicy: .deleteSyncedRows,
                fetch: { _ in await rows.all() },
                onUpdate: { _ in throw SampleError.rejected }
            )
        )

        await collection.start()
        let transaction = try await collection.update("todo-2") { todo in
            todo.title = "Local"
        }
        do {
            try await transaction.awaitCompletion()
            Issue.record("Expected failed mutation")
        } catch {
            #expect(Bool(true))
        }

        await rows.set([
            testTodoCollectionRow(id: "todo-1", title: "One"),
        ])
        await collection.refresh()

        let context = ModelContext(container)
        let preserved = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-2")).first)
        #expect(preserved.title == "Local")
        #expect(preserved.collectionSyncState == .syncError)
    }

    @Test("Keep local rows policy preserves missing rows")
    func keepLocalRowsPreservesMissingRows() async throws {
        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "One"),
            testTodoCollectionRow(id: "todo-2", title: "Two"),
        ])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                missingRowPolicy: .keepLocalRows,
                fetch: { _ in await rows.all() }
            )
        )

        await collection.start()
        await rows.set([
            testTodoCollectionRow(id: "todo-1", title: "One"),
        ])
        await collection.refresh()

        let context = ModelContext(container)
        #expect(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-2")).count == 1)
    }

    @Test("Successful mutation completes after automatic refresh")
    func successfulMutationCompletesAfterRefresh() async throws {
        let rows = TestFetchRows([])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() },
                onInsert: { context in
                    let inserted = context.mutations.compactMap(\.modified)
                    await rows.set(inserted)
                }
            )
        )

        await collection.start()
        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }
        try await transaction.awaitCompletion()

        let context = ModelContext(container)
        let pending = try #require(context.fetch(FetchDescriptor<PendingCollectionMutation>()).first)
        let todo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(pending.status == .resolved)
        #expect(todo.title == "Inserted")
        #expect(todo.collectionSyncState == .synced)
        #expect(todo.collectionPendingMutationCount == 0)
    }

    @Test("Failed mutation keeps existing retryable outbox behavior")
    func failedMutationKeepsRetryableOutboxBehavior() async throws {
        enum SampleError: Error { case sendFailed }

        let rows = TestFetchRows([])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() },
                onInsert: { _ in throw SampleError.sendFailed }
            )
        )

        await collection.start()
        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }

        do {
            try await transaction.awaitCompletion()
            Issue.record("Expected failed mutation")
        } catch {
            #expect(Bool(true))
        }

        let context = ModelContext(container)
        let pending = try #require(context.fetch(FetchDescriptor<PendingCollectionMutation>()).first)
        let todo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(pending.status == .failed)
        #expect(todo.collectionSyncState == .syncError)
    }

    @Test("Repeated identical refreshes are idempotent")
    func repeatedIdenticalRefreshesAreIdempotent() async throws {
        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "One"),
        ])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() }
            )
        )

        await collection.start()
        await collection.refresh()
        await collection.refresh()

        let context = ModelContext(container)
        let fetched = try context.fetch(FetchDescriptor<TestTodo>())
        #expect(fetched.count == 1)
        #expect(fetched.first?.title == "One")
    }

    @Test("Fetch adapter source id is deterministic and internal")
    func fetchAdapterSourceIDIsDeterministic() async throws {
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                identifier: testTodoIdentifier,
                fetch: { _ in [] }
            )
        )

        #expect(collection.sourceID == "fetch:\(String(reflecting: TestTodo.self))")
    }
}

actor TestFetchRows {
    private var rows: [CollectionRow]

    init(_ rows: [CollectionRow]) {
        self.rows = rows
    }

    func all() -> [CollectionRow] {
        rows
    }

    func set(_ rows: [CollectionRow]) {
        self.rows = rows
    }
}

