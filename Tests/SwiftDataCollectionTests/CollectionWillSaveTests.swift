@testable import ElectricSwiftDataCollection
@testable import FetchSwiftDataCollection
@testable import SwiftDataCollection
@testable import ElectricSwift
import Foundation
import SwiftData
import Testing

/// One observed `willSave` call: the `TestTodo` rows the save was about to write.
struct WillSaveCall: Sendable {
    struct Row: Sendable, Hashable {
        let id: String
        let title: String
        let syncState: CollectionSyncState
    }

    let inserted: [Row]
    let changed: [Row]
    let deletedIDs: [String]

    var written: [Row] { inserted + changed }
}

/// Records every hook call and, like an app deriving local state, writes a
/// `TestEvent` mirror of each written todo into the same save.
final class WillSaveRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storedCalls: [WillSaveCall] = []
    private let failure: (any Error)?

    init(failing failure: (any Error)? = nil) {
        self.failure = failure
    }

    var calls: [WillSaveCall] {
        lock.lock()
        defer { lock.unlock() }
        return storedCalls
    }

    var handler: CollectionWillSaveHandler {
        { [self] context in try record(context) }
    }

    private func record(_ context: ModelContext) throws {
        func rows(_ models: [any PersistentModel]) -> [WillSaveCall.Row] {
            models.compactMap { $0 as? TestTodo }.map {
                WillSaveCall.Row(id: $0.id, title: $0.title, syncState: $0.collectionSyncState)
            }
        }
        let call = WillSaveCall(
            inserted: rows(context.insertedModelsArray),
            changed: rows(context.changedModelsArray),
            deletedIDs: context.deletedModelsArray.compactMap { ($0 as? TestTodo)?.id }
        )
        lock.lock()
        storedCalls.append(call)
        lock.unlock()

        for row in call.written {
            let mirrorID = "mirror-\(row.id)"
            let existing = try context.fetch(FetchDescriptor<TestEvent>(predicate: #Predicate { $0.id == mirrorID })).first
            if let existing {
                existing.title = row.title
            } else {
                context.insert(TestEvent(id: mirrorID, title: row.title, startTime: Date(timeIntervalSince1970: 0)))
            }
        }
        if let failure { throw failure }
    }
}

private func mirrorTitle(of id: String, in container: ModelContainer) throws -> String? {
    let mirrorID = "mirror-\(id)"
    return try ModelContext(container)
        .fetch(FetchDescriptor<TestEvent>(predicate: #Predicate { $0.id == mirrorID }))
        .first?.title
}

private func todoTitle(of id: String, in container: ModelContainer) throws -> String? {
    try ModelContext(container).fetch(testTodoIdentifier.fetchDescriptor(for: id)).first?.title
}

private enum HookTestError: Error { case refused, commitFailed }

@Suite("Will-save hook")
struct CollectionWillSaveTests {
    @Test("A local insert runs the hook inside its commit")
    func localInsertRunsHookInsideCommit() async throws {
        let container = try makeTestContainer()
        let recorder = WillSaveRecorder()
        let store = SwiftDataCollectionStore(modelContainer: container, willSave: recorder.handler)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in .immediate }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        }

        let insertCall = try #require(recorder.calls.first { $0.inserted.contains { $0.id == "todo-1" } })
        #expect(insertCall.inserted.first?.title == "Local")
        #expect(try todoTitle(of: "todo-1", in: container) == "Local")
        #expect(try mirrorTitle(of: "todo-1", in: container) == "Local")
    }

    @Test("Local update and delete run the hook")
    func localUpdateAndDeleteRunHook() async throws {
        let container = try makeTestContainer()
        let recorder = WillSaveRecorder()
        let store = SwiftDataCollectionStore(modelContainer: container, willSave: recorder.handler)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in .immediate },
            onUpdate: { _ in .immediate },
            onDelete: { _ in .immediate }
        )
        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        }

        let beforeUpdate = recorder.calls.count
        _ = try await collection.update("todo-1") { $0.title = "Edited" }
        let updateCalls = recorder.calls.dropFirst(beforeUpdate)
        #expect(updateCalls.contains { $0.changed.contains { $0.id == "todo-1" && $0.title == "Edited" } })
        #expect(try mirrorTitle(of: "todo-1", in: container) == "Edited")

        let beforeDelete = recorder.calls.count
        _ = try await collection.delete("todo-1")
        #expect(recorder.calls.count > beforeDelete)
    }

    @Test("A failed commit discards the hook's changes with the row")
    func failedCommitDiscardsHookChanges() async throws {
        let container = try makeTestContainer()
        let recorder = WillSaveRecorder()
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            commitSave: { _ in throw HookTestError.commitFailed },
            willSave: recorder.handler
        )
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in .immediate }
        )

        await #expect(throws: HookTestError.commitFailed) {
            _ = try await collection.insert {
                TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
            }
        }

        #expect(recorder.calls.contains { $0.inserted.contains { $0.id == "todo-1" } })
        #expect(try todoTitle(of: "todo-1", in: container) == nil)
        #expect(try mirrorTitle(of: "todo-1", in: container) == nil)
    }

    @Test("A sync batch runs the hook before its single save")
    func syncBatchRunsHookBeforeSave() throws {
        let container = try makeTestContainer()
        let recorder = WillSaveRecorder()
        let context = ModelContext(container)
        let batch = testTodoBatch(
            messages: [
                testTodoMessage(value: testTodoRow(title: "Server"), operation: .insert),
                testTodoMessage(
                    key: "\"public\".\"todos\"/todo-2",
                    value: testTodoRow(id: "todo-2", title: "Second"),
                    operation: .insert
                ),
            ],
            offset: "7_0"
        )

        _ = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier, willSave: recorder.handler)
            .apply(batch, shapeID: "todos", in: context)

        let call = try #require(recorder.calls.first)
        #expect(Set(call.inserted.map(\.id)) == ["todo-1", "todo-2"])
        #expect(try mirrorTitle(of: "todo-1", in: container) == "Server")
        #expect(try mirrorTitle(of: "todo-2", in: container) == "Second")
        let metadata = try ModelContext(container).fetch(FetchDescriptor<ElectricShapeMetadata>()).first
        #expect(metadata?.offset == "7_0")
    }

    @Test("A throwing hook leaves the batch and its checkpoint unsaved")
    func throwingHookAbortsBatch() throws {
        let container = try makeTestContainer()
        let recorder = WillSaveRecorder(failing: HookTestError.refused)
        let context = ModelContext(container)
        let batch = testTodoBatch(
            messages: [testTodoMessage(value: testTodoRow(title: "Server"), operation: .insert)],
            offset: "9_0"
        )

        #expect(throws: HookTestError.refused) {
            _ = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier, willSave: recorder.handler)
                .apply(batch, shapeID: "todos", in: context)
        }

        #expect(try todoTitle(of: "todo-1", in: container) == nil)
        #expect(try mirrorTitle(of: "todo-1", in: container) == nil)
        #expect(try ModelContext(container).fetch(FetchDescriptor<ElectricShapeMetadata>()).isEmpty)
    }

    @Test("The hook sees the materialized row with pending local intent applied")
    func hookSeesOptimisticOverlay() throws {
        let container = try makeTestContainer()
        let setup = ModelContext(container)
        setup.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Server v1"))
        try insertTransactionBackedMutation(
            in: setup,
            targetKey: "todo-1",
            operation: .update,
            payload: testTodoCollectionRow(title: "Local"),
            changedFields: ["title"],
            baseline: testTodoCollectionRow(title: "Server v1")
        )
        try setup.save()

        let recorder = WillSaveRecorder()
        let batch = testTodoBatch(
            messages: [testTodoMessage(value: testTodoRow(title: "Server v2"), operation: .update)]
        )
        _ = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier, willSave: recorder.handler)
            .apply(batch, shapeID: "todos", in: ModelContext(container))

        let written = try #require(recorder.calls.first?.written.first { $0.id == "todo-1" })
        #expect(written.title == "Local")
        #expect(try mirrorTitle(of: "todo-1", in: container) == "Local")
        #expect(try todoTitle(of: "todo-1", in: container) == "Local")
    }

    @Test("Confirmation runs the hook when it rematerializes a row")
    func confirmationRunsHook() throws {
        let container = try makeTestContainer()
        let setup = ModelContext(container)
        let modelName = String(reflecting: TestTodo.self)
        let collectionID = "\(modelName):todos"
        setup.insert(TestTodo(
            collectionSyncState: .pendingCreate,
            collectionPendingMutationCount: 1,
            id: "todo-1",
            projectID: "project-a",
            title: "Local"
        ))
        let mutation = try insertTransactionBackedMutation(
            in: setup,
            targetKey: "todo-1",
            operation: .create,
            payload: testTodoCollectionRow(title: "Local"),
            status: .resolved,
            baseline: testTodoCollectionRow(title: "Local")
        )
        try setup.save()

        let recorder = WillSaveRecorder()
        let resolved = try CollectionMutationReconciler(modelContainer: container, willSave: recorder.handler)
            .resolveTransaction(id: mutation.transactionID, collectionID: collectionID) { context, keys in
                try CollectionMaterializer(
                    context: context,
                    collectionID: collectionID,
                    modelName: modelName,
                    identifier: testTodoIdentifier,
                    rowDecoder: .init()
                ).materialize(keys: keys)
            }

        #expect(resolved)
        let written = try #require(recorder.calls.first?.written.first { $0.id == "todo-1" })
        #expect(written.syncState == .synced)
    }

    @Test("Staged insert, publication and discard run the hook")
    func stagedWritesRunHook() async throws {
        let container = try makeTestContainer()
        let recorder = WillSaveRecorder()
        let store = SwiftDataCollectionStore(modelContainer: container, willSave: recorder.handler)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in .immediate }
        )

        _ = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Staged")
        }
        #expect(recorder.calls.contains {
            $0.inserted.contains { $0.id == "todo-1" && $0.syncState == .stagedCreate }
        })

        let beforePublish = recorder.calls.count
        _ = try await collection.publishStagedInsert("todo-1")
        #expect(recorder.calls.dropFirst(beforePublish).contains { $0.changed.contains { $0.id == "todo-1" } })

        _ = try await collection.stageInsert {
            TestTodo(id: "todo-2", projectID: "project-a", title: "Discarded")
        }
        let beforeDiscard = recorder.calls.count
        try await collection.discardStagedInsert("todo-2")
        #expect(recorder.calls.dropFirst(beforeDiscard).contains { $0.deletedIDs.contains("todo-2") })
    }

    @Test("A fetch adapter refresh runs the hook")
    func fetchRefreshRunsHook() async throws {
        let rows = TestFetchRows([
            testTodoCollectionRow(id: "todo-1", title: "One"),
            testTodoCollectionRow(id: "todo-2", title: "Two"),
        ])
        let container = try makeTestContainer()
        let recorder = WillSaveRecorder()
        let store = SwiftDataCollectionStore(modelContainer: container, willSave: recorder.handler)
        let collection = try await store.collection(
            TestTodo.self,
            options: fetchCollectionOptions(
                scopeID: "all",
                identifier: testTodoIdentifier,
                fetch: { _ in await rows.all() }
            )
        )

        await collection.start()

        #expect(recorder.calls.contains { Set($0.inserted.map(\.id)) == ["todo-1", "todo-2"] })
        #expect(try mirrorTitle(of: "todo-2", in: container) == "Two")
    }

    @Test("A shape-only row applier runs the hook")
    func shapeRowApplierRunsHook() throws {
        let container = try makeTestContainer()
        let recorder = WillSaveRecorder()
        let batch = testTodoBatch(
            messages: [testTodoMessage(value: testTodoRow(title: "Shape"), operation: .insert)]
        )

        _ = try ElectricSwiftDataRowApplier(identifier: testTodoIdentifier, willSave: recorder.handler)
            .apply(batch, shapeID: "todos", in: ModelContext(container))

        #expect(recorder.calls.first?.inserted.map(\.id) == ["todo-1"])
        #expect(try mirrorTitle(of: "todo-1", in: container) == "Shape")
    }
}
