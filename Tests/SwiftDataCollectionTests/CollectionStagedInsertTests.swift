@testable import ElectricSwiftDataCollection
@testable import ElectricSwift
@testable import FetchSwiftDataCollection
@testable import SwiftDataCollection
import SwiftData
import Testing

@Suite("Collection staged inserts")
struct CollectionStagedInsertTests {
    @Test("Staging persists without creating an outbox mutation and is idempotent")
    func stagePersistsWithoutOutbox() async throws {
        let container = try makeTestContainer()
        let store = ElectricCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        let inserted = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "First")
        }
        let repeated = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-b", title: "Replacement")
        }

        #expect(inserted == .inserted)
        #expect(repeated == .alreadyStaged)
        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "First")
        #expect(row.collectionSyncState == .stagedCreate)
        #expect(row.collectionPendingMutationCount == 0)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingTransaction>()).isEmpty)
    }

    @Test("Staged update remains local and preserves staged state")
    func updateStagedRemainsLocal() async throws {
        let container = try makeTestContainer()
        let store = ElectricCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )
        _ = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "First")
        }

        try await collection.updateStaged("todo-1") { $0.title = "Latest" }

        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Latest")
        #expect(row.collectionSyncState == .stagedCreate)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)
    }

    @Test("Publishing promotes the existing staged model into the durable outbox")
    func publishPromotesExistingModel() async throws {
        let container = try makeTestContainer()
        let store = ElectricCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in .txid(101) }
        )
        _ = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "First")
        }
        try await collection.updateStaged("todo-1") { $0.title = "Latest" }

        _ = try await collection.publishStagedInsert("todo-1")

        let context = ModelContext(container)
        let rows = try context.fetch(FetchDescriptor<TestTodo>())
        #expect(rows.count == 1)
        #expect(rows.first?.title == "Latest")
        #expect(rows.first?.collectionSyncState == .pendingCreate)
        let mutations = try context.fetch(FetchDescriptor<ElectricPendingMutation>())
        #expect(mutations.count == 1)
        #expect(mutations.first?.payload["title"] == .string("Latest"))
    }

    @Test("Discard removes only an eligible staged model")
    func discardRemovesStagedModel() async throws {
        let container = try makeTestContainer()
        let store = ElectricCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )
        _ = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "First")
        }

        try await collection.discardStagedInsert("todo-1")

        let context = ModelContext(container)
        #expect(try context.fetch(FetchDescriptor<TestTodo>()).isEmpty)
    }

    @Test("Staging a synchronized key does not downgrade it")
    func stageSyncedKeyIsNoOp() async throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Server"))
        try context.save()
        let store = ElectricCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        let outcome = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-b", title: "Local")
        }

        #expect(outcome == .alreadySynced)
        let fetched = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(fetched.title == "Server")
        #expect(fetched.collectionSyncState == .synced)
    }

    @Test("Electric insert resolves a staged row and Electric delete preserves staged work")
    func electricResolvesAndPreservesStagedRows() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(
            TestTodo(
                collectionSyncState: .stagedCreate,
                id: "todo-1",
                projectID: "local-project",
                title: "Local"
            )
        )
        context.insert(
            TestTodo(
                collectionSyncState: .stagedCreate,
                id: "todo-2",
                projectID: "local-project",
                title: "Keep"
            )
        )
        try context.save()

        let batch = testTodoBatch(
            messages: [
                testTodoMessage(
                    value: testTodoRow(
                        id: "todo-1",
                        projectID: "server-project",
                        title: "Server"
                    ),
                    operation: .insert
                ),
                testTodoMessage(
                    key: "\"public\".\"todos\"/todo-2",
                    operation: .delete
                ),
            ]
        )
        _ = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            batch,
            shapeID: "todos",
            in: context
        )

        let resolved = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(resolved.title == "Server")
        #expect(resolved.projectID == "server-project")
        #expect(resolved.collectionSyncState == .synced)
        let preserved = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-2")).first)
        #expect(preserved.title == "Keep")
        #expect(preserved.collectionSyncState == .stagedCreate)
    }

    @Test("Electric sparse update patches a staged row and refetch cleanup preserves it")
    func electricUpdatePatchesStagedRow() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(
            TestTodo(
                collectionSyncState: .stagedCreate,
                id: "todo-1",
                projectID: "local-project",
                title: "Local"
            )
        )
        try context.save()

        let batch = testTodoBatch(
            messages: [
                testTodoMessage(control: .mustRefetch),
                testTodoMessage(value: ["title": .string("Server")], operation: .update),
            ]
        )
        _ = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            batch,
            shapeID: "todos",
            in: context
        )

        let resolved = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(resolved.title == "Server")
        #expect(resolved.projectID == "local-project")
        #expect(resolved.collectionSyncState == .synced)
    }

    @Test("Standalone Electric application preserves staged rows on delete and refetch")
    func standaloneElectricPreservesStagedRows() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(
            TestTodo(
                collectionSyncState: .stagedCreate,
                id: "todo-1",
                projectID: "project-a",
                title: "Local"
            )
        )
        try context.save()

        _ = try ElectricSwiftDataRowApplier(identifier: testTodoIdentifier).apply(
            testTodoBatch(messages: [
                testTodoMessage(control: .mustRefetch),
                testTodoMessage(operation: .delete),
            ]),
            shapeID: "todos",
            in: context
        )

        let preserved = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(preserved.collectionSyncState == .stagedCreate)
    }

    @Test("Fetch snapshots resolve returned staged rows and preserve missing staged rows")
    func fetchSnapshotHandlesStagedRows() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(
            TestTodo(
                collectionSyncState: .stagedCreate,
                id: "todo-1",
                projectID: "local-project",
                title: "Local"
            )
        )
        context.insert(
            TestTodo(
                collectionSyncState: .stagedCreate,
                id: "todo-2",
                projectID: "local-project",
                title: "Keep"
            )
        )
        try context.save()

        _ = try FetchCollectionSnapshotApplier(
            identifier: testTodoIdentifier,
            rowDecoder: .init(),
            modelName: String(reflecting: TestTodo.self),
            missingRowPolicy: .deleteSyncedRows
        ).apply(
            [testTodoCollectionRow(id: "todo-1", projectID: "server-project", title: "Server")],
            in: context
        )

        let resolved = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(resolved.title == "Server")
        #expect(resolved.collectionSyncState == .synced)
        let preserved = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-2")).first)
        #expect(preserved.collectionSyncState == .stagedCreate)
    }

    @Test("Restart preserves staged state with no outbox mutation")
    func restartPreservesStagedState() async throws {
        let location = TestStoreLocation()
        defer { location.cleanup() }
        do {
            let container = try location.makeContainer()
            let store = ElectricCollectionStore(modelContainer: container)
            let collection = try await store.collection(
                TestTodo.self,
                identifier: testTodoIdentifier,
                table: "todos"
            )
            _ = try await collection.stageInsert {
                TestTodo(id: "todo-1", projectID: "project-a", title: "Durable")
            }
        }

        let reopened = try location.makeContainer()
        let store = ElectricCollectionStore(modelContainer: reopened)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )
        let outcome = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-b", title: "Replacement")
        }

        #expect(outcome == .alreadyStaged)
        let context = ModelContext(reopened)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Durable")
        #expect(row.collectionSyncState == .stagedCreate)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)
    }

    @Test("Staged rows can be discovered by filtering fetched models")
    func fetchAndFilterDiscoversStagedRows() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(
            TestTodo(
                collectionSyncState: .stagedCreate,
                id: "todo-1",
                projectID: "project-a",
                title: "Staged"
            )
        )
        context.insert(TestTodo(id: "todo-2", projectID: "project-a", title: "Synced"))
        try context.save()

        let staged = try context.fetch(FetchDescriptor<TestTodo>())
            .filter { $0.collectionSyncState == .stagedCreate }

        #expect(staged.map(\.id) == ["todo-1"])
    }

    @Test("Staged operations reject unresolved mutations without changing durable state")
    func stagingRejectsOrphanedMutation() async throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(
            try makePendingMutation(
                targetKey: "todo-1",
                operation: .create,
                payload: testTodoRow(),
                changedFields: ["id", "projectID", "title"]
            )
        )
        try context.save()
        let store = ElectricCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        do {
            _ = try await collection.stageInsert {
                TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
            }
            Issue.record("Expected orphaned pending mutation to reject staging")
        } catch CollectionError.stagedOperationHasPendingMutations(let key, let count) {
            #expect(key == "todo-1")
            #expect(count == 1)
        }

        #expect(try context.fetch(FetchDescriptor<TestTodo>()).isEmpty)
    }

    @Test("Changing a staged stable identifier rolls back atomically")
    func stagedUpdateRejectsIdentifierChange() async throws {
        let container = try makeTestContainer()
        let store = ElectricCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )
        _ = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        }

        do {
            try await collection.updateStaged("todo-1") { $0.id = "todo-2" }
            Issue.record("Expected stable identifier change to fail")
        } catch CollectionError.stableIdentifierChanged(let expected, let actual) {
            #expect(expected == "todo-1")
            #expect(actual == "todo-2")
        }

        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.id == "todo-1")
        #expect(row.collectionSyncState == .stagedCreate)
    }

    @Test("Failed publication leaves the row staged and the outbox empty")
    func failedPublicationRollsBackAtomically() async throws {
        let controller = StagedSaveController()
        let recorder = TestTraceRecorder()
        let container = try makeTestContainer()
        let store = ElectricCollectionStore(
            modelContainer: container,
            diagnostics: recorder.diagnostics(),
            commitSave: controller.save
        )
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )
        _ = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        }
        controller.failNextSave()

        await #expect(throws: StagedSaveFailure.self) {
            _ = try await collection.publishStagedInsert("todo-1")
        }

        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.collectionSyncState == .stagedCreate)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingTransaction>()).isEmpty)
        #expect(recorder.events.contains { $0.kind == .stagedInsertPublished } == false)
    }

    @Test("Failed stage, staged update, and discard saves roll back atomically")
    func failedStagedWritesRollbackAtomically() async throws {
        let controller = StagedSaveController()
        let recorder = TestTraceRecorder()
        let container = try makeTestContainer()
        let store = ElectricCollectionStore(
            modelContainer: container,
            diagnostics: recorder.diagnostics(),
            commitSave: controller.save
        )
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        controller.failNextSave()
        await #expect(throws: StagedSaveFailure.self) {
            _ = try await collection.stageInsert {
                TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
            }
        }
        var context = ModelContext(container)
        #expect(try context.fetch(FetchDescriptor<TestTodo>()).isEmpty)
        #expect(recorder.events.contains { $0.kind == .stagedInsertCreated } == false)

        _ = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        }
        controller.failNextSave()
        await #expect(throws: StagedSaveFailure.self) {
            try await collection.updateStaged("todo-1") { $0.title = "Changed" }
        }
        context = ModelContext(container)
        var row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Local")
        #expect(row.collectionSyncState == .stagedCreate)
        #expect(recorder.events.contains { $0.kind == .stagedInsertUpdated } == false)

        controller.failNextSave()
        await #expect(throws: StagedSaveFailure.self) {
            try await collection.discardStagedInsert("todo-1")
        }
        context = ModelContext(container)
        row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.collectionSyncState == .stagedCreate)
        #expect(recorder.events.contains { $0.kind == .stagedInsertDiscarded } == false)
    }

    @Test("Managed stage and adapter upsert races have deterministic final state")
    func managedStageAndAdapterRace() async throws {
        let container = try makeTestContainer()
        let controller = StagedTestAdapterController()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: CollectionOptions(
                debugName: "controlled",
                identifier: testTodoIdentifier,
                adapter: CollectionAdapter(
                    sourceID: "controlled",
                    makeRuntime: { context in
                        await controller.install(context)
                        return StagedTestAdapterRuntime()
                    }
                ),
                onInsert: { _ in }
            )
        )

        for index in 0..<20 {
            let key = "todo-\(index)"
            async let stageOutcome = collection.stageInsert {
                TestTodo(id: key, projectID: "local", title: "Local")
            }
            async let adapterApply: Void = controller.apply(
                testTodoCollectionRow(id: key, projectID: "server", title: "Server")
            )

            let outcome = try await stageOutcome
            try await adapterApply
            #expect(outcome == .inserted || outcome == .alreadySynced)
        }

        let context = ModelContext(container)
        let rows = try context.fetch(FetchDescriptor<TestTodo>())
        #expect(rows.count == 20)
        #expect(rows.allSatisfy { $0.collectionSyncState == .synced })
        #expect(rows.allSatisfy { $0.title == "Server" })
    }

    @Test("A cached context cannot make a stale staging decision")
    func cachedContextDoesNotDriveStagingDecision() async throws {
        let container = try makeTestContainer()
        let cachedContext = ModelContext(container)
        #expect(try cachedContext.fetch(FetchDescriptor<TestTodo>()).isEmpty)

        let controller = StagedTestAdapterController()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            options: CollectionOptions(
                debugName: "controlled",
                identifier: testTodoIdentifier,
                adapter: CollectionAdapter(
                    sourceID: "controlled",
                    makeRuntime: { context in
                        await controller.install(context)
                        return StagedTestAdapterRuntime()
                    }
                ),
                onInsert: { _ in }
            )
        )
        try await controller.apply(
            testTodoCollectionRow(id: "todo-1", projectID: "server", title: "Server")
        )

        let outcome = try await collection.stageInsert {
            TestTodo(id: "todo-1", projectID: "local", title: "Local")
        }

        #expect(outcome == .alreadySynced)
    }

    @Test("Managed Electric reset traces preserved staged rows after save")
    func managedElectricResetTracesPreservedStagedRow() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(
            TestTodo(
                collectionSyncState: .stagedCreate,
                id: "todo-1",
                projectID: "project-a",
                title: "Staged"
            )
        )
        try context.save()
        let recorder = TestTraceRecorder()
        let synchronizer = ElectricCollectionSynchronizer(
            identifier: testTodoIdentifier,
            collectionID: "TestTodo:todos",
            tracer: recorder.tracer()
        )

        _ = try synchronizer.apply(
            testTodoBatch(
                messages: [testTodoMessage(control: .mustRefetch)],
                offset: "12_0"
            ),
            shapeID: "todos",
            in: context
        )

        let event = try #require(
            recorder.events.first { $0.kind == .stagedDeletePreserved && $0.key == "todo-1" }
        )
        #expect(event.offset == "12_0")
        #expect(event.metadata["adapterOperation"] == "mustRefetch")
        #expect(event.metadata["outcome"] == "preservedStagedReset")
    }

    @Test("Managed publish, discard, and update races honor both forced orderings")
    func managedStagedOperationsHonorForcedOrderings() async throws {
        let container = try makeTestContainer()
        let controller = StagedTestAdapterController()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let insertHandler: CollectionAdapterMutationHandler<TestTodo, String> = { _ in
            .awaitTokens(["101"])
        }
        let options = CollectionOptions<TestTodo, String>(
            debugName: "controlled",
            identifier: testTodoIdentifier,
            adapter: CollectionAdapter(
                sourceID: "controlled",
                makeRuntime: { context in
                    await controller.install(context)
                    return StagedTestAdapterRuntime()
                }
            ),
            onInsert: insertHandler
        )
        let collection = try await store.collection(TestTodo.self, options: options)

        _ = try await collection.stageInsert {
            TestTodo(id: "stage-first", projectID: "local", title: "Local")
        }
        try await controller.apply(
            testTodoCollectionRow(id: "stage-first", projectID: "server", title: "Server")
        )

        _ = try await collection.stageInsert {
            TestTodo(id: "publish-first", projectID: "local", title: "Local")
        }
        _ = try await collection.publishStagedInsert("publish-first")
        try await controller.apply(
            testTodoCollectionRow(id: "publish-first", projectID: "server", title: "Server")
        )

        _ = try await collection.stageInsert {
            TestTodo(id: "adapter-first", projectID: "local", title: "Local")
        }
        try await controller.apply(
            testTodoCollectionRow(id: "adapter-first", projectID: "server", title: "Server")
        )
        do {
            _ = try await collection.publishStagedInsert("adapter-first")
            Issue.record("Expected adapter-first publication to fail")
        } catch CollectionError.invalidStagedTransition(let key, let state) {
            #expect(key == "adapter-first")
            #expect(state == .synced)
        }

        _ = try await collection.stageInsert {
            TestTodo(id: "discard-first", projectID: "local", title: "Local")
        }
        try await collection.discardStagedInsert("discard-first")
        try await controller.apply(
            testTodoCollectionRow(id: "discard-first", projectID: "server", title: "Recreated")
        )

        _ = try await collection.stageInsert {
            TestTodo(id: "discard-adapter-first", projectID: "local", title: "Local")
        }
        try await controller.apply(
            testTodoCollectionRow(id: "discard-adapter-first", projectID: "server", title: "Server")
        )
        await #expect(throws: CollectionError.self) {
            try await collection.discardStagedInsert("discard-adapter-first")
        }

        _ = try await collection.stageInsert {
            TestTodo(id: "update-first", projectID: "local", title: "Local")
        }
        try await collection.updateStaged("update-first") { $0.title = "Latest Local" }
        try await controller.apply(
            testTodoCollectionRow(id: "update-first", projectID: "server", title: "Server")
        )

        _ = try await collection.stageInsert {
            TestTodo(id: "update-adapter-first", projectID: "local", title: "Local")
        }
        try await controller.apply(
            testTodoCollectionRow(id: "update-adapter-first", projectID: "server", title: "Server")
        )
        await #expect(throws: CollectionError.self) {
            try await collection.updateStaged("update-adapter-first") { $0.title = "Too Late" }
        }

        let context = ModelContext(container)
        let published = try #require(
            context.fetch(testTodoIdentifier.fetchDescriptor(for: "publish-first")).first
        )
        #expect(published.collectionSyncState == .pendingCreate)
        let recreated = try #require(
            context.fetch(testTodoIdentifier.fetchDescriptor(for: "discard-first")).first
        )
        #expect(recreated.collectionSyncState == .synced)
        #expect(recreated.title == "Recreated")
        let updated = try #require(
            context.fetch(testTodoIdentifier.fetchDescriptor(for: "update-first")).first
        )
        #expect(updated.collectionSyncState == .synced)
        #expect(updated.title == "Server")
    }
}

private actor StagedTestAdapterRuntime: CollectionAdapterRuntime {
    func start() async {}
    func stop() async {}
    func refresh() async {}
}

private enum StagedSaveFailure: Error {
    case save
}

private final class StagedSaveController: @unchecked Sendable {
    private let lock = NSLock()
    private var shouldFailNextSave = false

    func failNextSave() {
        lock.lock()
        shouldFailNextSave = true
        lock.unlock()
    }

    func save(_ context: ModelContext) throws {
        lock.lock()
        let fail = shouldFailNextSave
        shouldFailNextSave = false
        lock.unlock()
        if fail { throw StagedSaveFailure.save }
        try context.save()
    }
}

private actor StagedTestAdapterController {
    private var applyClosure: (@Sendable (CollectionRow) throws -> Void)?

    func install(_ adapterContext: CollectionAdapterContext<TestTodo, String>) {
        applyClosure = { row in
            try adapterContext.writeGate.withCriticalSection {
                let context = ModelContext(adapterContext.modelContainer)
                let decoded = try TestTodo(collectionRow: row, decoder: adapterContext.rowDecoder)
                let key = adapterContext.identifier.key(for: decoded)
                let outcome = try CollectionStagedReconciler.applyUpsert(
                    key: key,
                    row: row,
                    mode: .replacement,
                    identifier: adapterContext.identifier,
                    rowDecoder: adapterContext.rowDecoder,
                    in: context
                )
                if outcome == .notStaged {
                    if let existing = try context.fetch(
                        adapterContext.identifier.fetchDescriptor(forSerializedKey: key)
                    ).first {
                        try existing.apply(
                            collectionRow: row,
                            decoder: adapterContext.rowDecoder
                        )
                    } else {
                        decoded.collectionSyncState = .synced
                        decoded.collectionPendingMutationCount = 0
                        context.insert(decoded)
                    }
                }
                try context.save()
            }
        }
    }

    func apply(_ row: CollectionRow) throws {
        guard let applyClosure else {
            throw CollectionTransactionFailure(message: "controlled adapter was not installed")
        }
        try applyClosure(row)
    }
}
