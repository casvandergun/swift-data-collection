@testable import FetchSwiftDataCollection
@testable import SwiftDataCollection
import Foundation
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
                scopeID: "all",
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
                scopeID: "all",
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
                scopeID: "all",
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
                scopeID: "all",
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
            try await transaction.wait()
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
                scopeID: "all",
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
                scopeID: "all",
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
        try await transaction.wait()

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
                scopeID: "all",
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
            try await transaction.wait()
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

    @Test("Fetched stale row preserves pending update changed fields")
    func fetchedStaleRowPreservesPendingUpdateChangedFields() async throws {
        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", projectID: "project-a", title: "Original"),
        ])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                scopeID: "all",
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() },
                onUpdate: { _ in
                    await rows.set([
                        testTodoCollectionRow(
                            id: "todo-1",
                            projectID: "server-project",
                            title: "Original"
                        ),
                    ])
                }
            )
        )

        await collection.start()
        let transaction = try await collection.update("todo-1") { todo in
            todo.title = "Local"
        }
        try await transaction.wait()

        let context = ModelContext(container)
        let todo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(todo.title == "Local")
        #expect(todo.projectID == "server-project")
        #expect(todo.collectionSyncState == .synced)
    }

    @Test("Fetched row does not mark pending create synced")
    func fetchedRowDoesNotMarkPendingCreateSynced() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let local = TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        local.collectionSyncState = .pendingCreate
        local.collectionPendingMutationCount = 1
        context.insert(local)
        context.insert(
            try testPendingMutation(
                key: "todo-1",
                operation: .create,
                payload: testTodoCollectionRow(id: "todo-1", projectID: "project-a", title: "Local"),
                changedFields: ["id", "projectID", "title"],
                status: .awaitingSync
            )
        )
        try context.save()

        _ = try makeTestFetchApplier().apply(
            [
                testTodoCollectionRow(id: "todo-1", projectID: "server-project", title: "Server"),
            ],
            in: context
        )

        let todo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(todo.title == "Local")
        #expect(todo.projectID == "project-a")
        #expect(todo.collectionSyncState == .pendingCreate)
        #expect(todo.collectionPendingMutationCount == 1)
    }

    @Test("Fetched row does not rehydrate pending delete")
    func fetchedRowDoesNotRehydratePendingDelete() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let local = TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        local.collectionSyncState = .pendingDelete
        local.collectionPendingMutationCount = 1
        context.insert(local)
        context.insert(
            try testPendingMutation(
                key: "todo-1",
                operation: .delete,
                payload: [:],
                status: .awaitingSync
            )
        )
        try context.save()

        _ = try makeTestFetchApplier().apply(
            [
                testTodoCollectionRow(id: "todo-1", projectID: "server-project", title: "Server"),
            ],
            in: context
        )

        let todo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(todo.title == "Local")
        #expect(todo.projectID == "project-a")
        #expect(todo.collectionSyncState == .pendingDelete)
        #expect(todo.collectionPendingMutationCount == 1)
    }

    @Test("Fetched row preserves failed mutation local state")
    func fetchedRowPreservesFailedMutationLocalState() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let local = TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        local.collectionSyncState = .syncError
        local.collectionPendingMutationCount = 1
        context.insert(local)
        context.insert(
            try testPendingMutation(
                key: "todo-1",
                operation: .update,
                payload: testTodoCollectionRow(id: "todo-1", projectID: "project-a", title: "Local"),
                changedFields: ["title"],
                status: .failed
            )
        )
        try context.save()

        _ = try makeTestFetchApplier().apply(
            [
                testTodoCollectionRow(id: "todo-1", projectID: "server-project", title: "Server"),
            ],
            in: context
        )

        let todo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(todo.title == "Local")
        #expect(todo.projectID == "project-a")
        #expect(todo.collectionSyncState == .syncError)
        #expect(todo.collectionPendingMutationCount == 1)
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
                scopeID: "all",
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

    @Test("Batch applied callback runs after snapshot save and persists explicit follow-up changes")
    func batchAppliedCallbackRunsAfterSnapshotSaveAndPersistsExplicitChanges() async throws {
        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "One"),
            testTodoCollectionRow(id: "todo-2", title: "Two"),
        ])
        let recorder = TestBatchAppliedRecorder()
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                scopeID: "all",
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() },
                onApply: { context, summary in
                    let currentRows = try context.fetch(FetchDescriptor<TestTodo>())
                    recorder.record(summary: summary, rowCount: currentRows.count)
                    if let first = currentRows.first(where: { $0.id == "todo-1" }) {
                        first.title = "Hydrated"
                    }
                    try context.save()
                }
            )
        )

        await collection.start()

        let context = ModelContext(container)
        let hydrated = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        let summary = try #require(recorder.summaries().first)
        #expect(hydrated.title == "Hydrated")
        #expect(recorder.rowCounts() == [2])
        #expect(summary.collectionIdentifier == "\(String(reflecting: TestTodo.self)):\(collection.sourceID)")
        #expect(summary.sourceIdentifier == collection.sourceID)
        #expect(summary.insertedCount == 2)
        #expect(summary.updatedCount == 0)
        #expect(summary.deletedCount == 0)
        #expect(summary.observedTXIDs == [])
    }

    @Test("Throwing batch applied callback reports collection error")
    func throwingBatchAppliedCallbackReportsCollectionError() async throws {
        enum SampleError: Error { case hydrationFailed }

        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "One"),
        ])
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                scopeID: "all",
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() },
                onApply: { _, _ in
                    throw SampleError.hydrationFailed
                }
            )
        )

        await collection.start()

        switch await collection.status {
        case .error(let message):
            #expect(message.contains("hydrationFailed"))
        case let status:
            Issue.record("Expected callback failure to report collection error, got \(status)")
        }
    }

    @Test("Fetch adapter source id includes scope id")
    func fetchAdapterSourceIDIncludesScopeID() async throws {
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                scopeID: "project:a",
                identifier: testTodoIdentifier,
                fetch: { _ in [] }
            )
        )

        #expect(collection.sourceID == "fetch:\(String(reflecting: TestTodo.self)):project:a")
    }

    @Test("Different scope ids produce different source ids")
    func differentScopeIDsProduceDifferentSourceIDs() {
        let first = fetchCollectionOptions(
            scopeID: "project:a",
            identifier: testTodoIdentifier,
            fetch: { _ in [] }
        )
        let second = fetchCollectionOptions(
            scopeID: "project:b",
            identifier: testTodoIdentifier,
            fetch: { _ in [] }
        )

        #expect(first.adapter.sourceID == "fetch:\(String(reflecting: TestTodo.self)):project:a")
        #expect(second.adapter.sourceID == "fetch:\(String(reflecting: TestTodo.self)):project:b")
        #expect(first.adapter.sourceID != second.adapter.sourceID)
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

func makeTestFetchApplier() -> FetchCollectionSnapshotApplier<TestTodo, String> {
    FetchCollectionSnapshotApplier(
        identifier: testTodoIdentifier,
        rowDecoder: .init(),
        modelName: String(reflecting: TestTodo.self),
        missingRowPolicy: .deleteSyncedRows
    )
}

func testPendingMutation(
    key: String,
    operation: CollectionMutationOperation,
    payload: CollectionRow,
    changedFields: Set<String> = [],
    status: PendingMutationStatus
) throws -> PendingCollectionMutation {
    PendingCollectionMutation(
        transactionID: UUID(),
        modelName: String(reflecting: TestTodo.self),
        shapeID: "fetch:\(String(reflecting: TestTodo.self)):all",
        targetKey: key,
        operation: operation,
        payloadData: try JSONEncoder().encode(payload),
        changedFieldsData: try JSONEncoder().encode(changedFields),
        status: status
    )
}
