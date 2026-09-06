@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection
import Foundation
import SwiftData
import Testing

@Suite("Collection conflict repair")
struct CollectionConflictTests {
    actor MutationRecorder {
        private var rejectNext = true
        private var calls: [CollectionMutation] = []

        init(rejectNext: Bool = true) {
            self.rejectNext = rejectNext
        }

        func submit(
            _ context: CollectionMutationContext<TestTodo, String>
        ) throws -> ElectricMutationSubmission {
            let mutation = context.mutations[0]
            calls.append(mutation)
            if rejectNext {
                rejectNext = false
                throw CollectionNonRetriableError("server refused mutation")
            }
            return .immediate
        }

        func count() -> Int { calls.count }

        func allCalls() -> [CollectionMutation] { calls }
    }

    @Test("Discarding an update conflict repairs a queued successor")
    func discardUpdateConflictRepairsSuccessor() async throws {
        let container = try makeTestContainer()
        let setup = ModelContext(container)
        setup.insert(TestTodo(id: "todo-1", projectID: "server-project", title: "Server"))
        try setup.save()

        let recorder = MutationRecorder()
        let database = ElectricCollectionStore(
            shapeURL: testElectricCollectionShapeURL,
            modelContainer: container
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { context in try await recorder.submit(context) }
        )

        let refused = try await collection.update("todo-1") { todo in
            todo.title = "Refused"
        }
        try await waitUntil { await recorder.count() == 1 }
        #expect(await refused.status == .failed("server refused mutation"))

        let initialConflicts = try await collection.conflicts()
        let conflict = try #require(initialConflicts.first)
        #expect(conflict.repairReadiness == .ready)
        #expect(conflict.entries == [
            CollectionConflictEntry(
                key: "todo-1",
                operation: .update,
                localChanges: ["title": .string("Refused")],
                baselineEvidence: .observedRow(
                    testTodoCollectionRow(
                        id: "todo-1",
                        projectID: "server-project",
                        title: "Server"
                    )
                )
            )
        ])

        let successor = try await collection.update("todo-1") { todo in
            todo.projectID = "successor-project"
        }
        #expect(await recorder.count() == 1)

        try await collection.discard(conflict.id)
        try await successor.wait()

        let calls = await recorder.allCalls()
        #expect(calls.count == 2)
        let successorCall = try #require(calls.last)
        #expect(successorCall.changes == ["projectID": .string("successor-project")])
        #expect(successorCall.modified?[
            "title"
        ] == .string("Server"))

        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Server")
        #expect(row.projectID == "successor-project")
        #expect(row.collectionSyncState == .synced)
        #expect(row.collectionPendingMutationCount == 0)

        let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
        #expect(transactions.map(\.status) == [.discarded, .resolved])
        #expect(try await collection.conflicts().isEmpty)
    }

    @Test("Independent conflict streams receive committed snapshots")
    func independentStreamsReceiveSnapshots() async throws {
        let container = try makeTestContainer()
        let setup = ModelContext(container)
        setup.insert(TestTodo(id: "todo-1", projectID: "server-project", title: "Server"))
        try setup.save()

        let recorder = MutationRecorder()
        let database = ElectricCollectionStore(
            shapeURL: testElectricCollectionShapeURL,
            modelContainer: container
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { context in try await recorder.submit(context) }
        )

        let stream1 = await collection.conflictUpdates
        var iterator1 = stream1.makeAsyncIterator()
        let first1 = try await iterator1.next()
        #expect(first1?.isEmpty == true)

        let stream2 = await collection.conflictUpdates
        var iterator2 = stream2.makeAsyncIterator()
        let first2 = try await iterator2.next()
        #expect(first2?.isEmpty == true)

        _ = try await collection.update("todo-1") { todo in
            todo.title = "Refused"
        }

        let conflict1 = try #require(try await iterator1.next()?.first)
        let conflict2 = try #require(try await iterator2.next()?.first)
        #expect(conflict1.id == conflict2.id)

        try await collection.discard(conflict1.id)
        let final1 = try await iterator1.next()
        let final2 = try await iterator2.next()
        #expect(final1?.isEmpty == true)
        #expect(final2?.isEmpty == true)
    }

    @Test("Discarding a compacted conflict group removes all source intent")
    func discardCompactedConflictGroup() async throws {
        let container = try makeTestContainer()
        let setup = ModelContext(container)
        setup.insert(TestTodo(id: "todo-1", projectID: "server-project", title: "Server"))
        try setup.save()

        let connectivity = TestConnectivityMonitor(initialState: .offline)
        let recorder = MutationRecorder()
        let database = SwiftDataCollectionStore(
            modelContainer: container,
            connectivityMonitor: connectivity
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { context in try await recorder.submit(context) }
        )

        _ = try await collection.update("todo-1") { todo in
            todo.title = "First refused"
        }
        _ = try await collection.update("todo-1") { todo in
            todo.title = "Second refused"
        }
        #expect(await recorder.count() == 0)

        connectivity.setState(.online)
        try await waitUntil {
            guard await recorder.count() == 1 else { return false }
            return try await collection.conflicts().isEmpty == false
        }

        let conflict = try #require(try await collection.conflicts().first)
        #expect(conflict.transactionIDs.count == 2)
        #expect(conflict.entries.count == 1)
        #expect(conflict.entries[0].localChanges == ["title": .string("Second refused")])

        try await collection.discard(conflict.id)
        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Server")
        #expect(row.collectionSyncState == .synced)
        #expect(try await collection.conflicts().isEmpty)
        #expect(await recorder.count() == 1)
    }

    @Test("Conflicts and discard survive a coordinator restart")
    func conflictSurvivesRestart() async throws {
        let location = TestStoreLocation()
        defer { location.cleanup() }

        let container = try location.makeContainer()
        let setup = ModelContext(container)
        setup.insert(TestTodo(id: "todo-1", projectID: "server-project", title: "Server"))
        try setup.save()

        let conflictID: UUID
        do {
            let recorder = MutationRecorder()
            let database = ElectricCollectionStore(
                shapeURL: testElectricCollectionShapeURL,
                modelContainer: container
            )
            let collection = try await database.collection(
                TestTodo.self,
                identifier: testTodoIdentifier,
                table: "todos",
                onUpdate: { context in try await recorder.submit(context) }
            )
            _ = try await collection.update("todo-1") { todo in
                todo.title = "Refused"
            }
            try await waitUntil { (try await collection.conflicts()).isEmpty == false }
            conflictID = try #require(try await collection.conflicts().first?.id)
        }

        let reopenedContainer = try location.makeContainer()
        let reopenedRecorder = MutationRecorder(rejectNext: false)
        let reopenedDatabase = ElectricCollectionStore(
            shapeURL: testElectricCollectionShapeURL,
            modelContainer: reopenedContainer
        )
        let reopenedCollection = try await reopenedDatabase.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { context in try await reopenedRecorder.submit(context) }
        )
        let persisted = try await reopenedCollection.conflicts()
        #expect(persisted.map(\.id) == [conflictID])
        try await reopenedCollection.discard(conflictID)
        #expect(await reopenedRecorder.count() == 0)

        let context = ModelContext(reopenedContainer)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Server")
        #expect(try await reopenedCollection.conflicts().isEmpty)
    }

    @Test("A dispatch preparation save failure stops the drain before invoking the handler")
    func dispatchPreparationSaveFailureDoesNotSpin() async throws {
        let container = try makeTestContainer()
        let saver = FailingPreparationSaver()
        let recorder = MutationRecorder(rejectNext: false)
        let database = ElectricCollectionStore(
            shapeURL: testElectricCollectionShapeURL,
            modelContainer: container,
            commitSave: { context in try saver.save(context) }
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { context in try await recorder.submit(context) }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Local")
        }

        #expect(await recorder.count() == 0)
        #expect(saver.failureCount() == 1)
        #expect(await transaction.status == .failed("prepare save failed"))

        let context = ModelContext(container)
        let persisted = try #require(context.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(persisted.status == .pending)
        #expect(persisted.dispatchGroupID == nil)
        #expect(persisted.submittedMutationsData == nil)
        #expect(persisted.attemptCount == 0)
    }

    @Test("A multi-key discard save failure leaves every key conflicted")
    func multiKeyDiscardIsAtomic() async throws {
        let container = try makeTestContainer()
        let setup = ModelContext(container)
        setup.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Server 1"))
        setup.insert(TestTodo(id: "todo-2", projectID: "project-a", title: "Server 2"))
        try setup.save()

        let saver = FailingDiscardSaver()
        let recorder = MutationRecorder()
        let database = ElectricCollectionStore(
            shapeURL: testElectricCollectionShapeURL,
            modelContainer: container,
            commitSave: { context in try saver.save(context) }
        )
        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { context in try await recorder.submit(context) }
        )

        let transaction = try await collection.transaction { builder in
            try builder.update("todo-1") { $0.title = "Local 1" }
            try builder.update("todo-2") { $0.title = "Local 2" }
        }
        do {
            try await transaction.wait()
            Issue.record("Expected the multi-key mutation to be refused")
        } catch {
            // Expected permanent refusal.
        }

        let conflict = try #require(try await collection.conflicts().first)
        #expect(conflict.entries.count == 2)
        saver.enable()
        do {
            try await collection.discard(conflict.id)
            Issue.record("Expected the injected discard save to fail")
        } catch {
            // Expected injected failure; the transaction must remain intact.
        }
        #expect(try await collection.conflicts().count == 1)

        saver.disable()
        try await collection.discard(conflict.id)
        #expect(try await collection.conflicts().isEmpty)
        let context = ModelContext(container)
        let rows = try context.fetch(FetchDescriptor<TestTodo>())
            .sorted { $0.id < $1.id }
        #expect(rows.map(\.title) == ["Server 1", "Server 2"])
        #expect(rows.allSatisfy { $0.collectionSyncState == .synced })
    }

    private final class FailingPreparationSaver: @unchecked Sendable {
        private let lock = NSLock()
        private var failures = 0

        func save(_ context: ModelContext) throws {
            let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
            if transactions.contains(where: { $0.status == .sending }) {
                lock.lock()
                failures += 1
                lock.unlock()
                throw PreparationSaveError()
            }
            try context.save()
        }

        func failureCount() -> Int {
            lock.lock()
            defer { lock.unlock() }
            return failures
        }
    }

    private struct PreparationSaveError: Error, Sendable, CustomStringConvertible {
        var description: String { "prepare save failed" }
    }

    private final class FailingDiscardSaver: @unchecked Sendable {
        private let lock = NSLock()
        private var shouldFail = false

        func enable() {
            lock.lock()
            shouldFail = true
            lock.unlock()
        }

        func disable() {
            lock.lock()
            shouldFail = false
            lock.unlock()
        }

        func save(_ context: ModelContext) throws {
            lock.lock()
            let fail = shouldFail
            lock.unlock()
            let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
            if fail && transactions.contains(where: { $0.status == .discarded }) {
                throw DiscardSaveError()
            }
            try context.save()
        }
    }

    private struct DiscardSaveError: Error, Sendable, CustomStringConvertible {
        var description: String { "discard save failed" }
    }
}
