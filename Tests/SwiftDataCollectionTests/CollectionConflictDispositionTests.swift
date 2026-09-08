import Foundation
import SwiftData
import Testing
@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection

/// A permanently refused mutation has two very different meanings, and the
/// disposition is how a handler tells them apart.
///
/// A server that judged the intent and rejected it has told you the client
/// lost, and holding that intent forever only accumulates work someone must
/// clear by hand. An expired token or a bad deploy produces the same status
/// code over content that is perfectly valid.
@Suite("Conflict Disposition")
struct CollectionConflictDispositionTests {
    actor ConflictCollector {
        private var conflicts: [CollectionConflict] = []
        func append(_ conflict: CollectionConflict) { conflicts.append(conflict) }
        func value() -> [CollectionConflict] { conflicts }
    }

    @Test("A refused insert is discarded by default and its optimistic row repaired")
    func discardIsTheDefault() async throws {
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in throw CollectionNonRetriableError("validation failed") }
        )

        _ = try? await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Refused")
        }

        #expect(try await collection.conflicts().isEmpty)

        let context = ModelContext(container)
        // The create was refused, so the row it optimistically added is gone.
        #expect(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first == nil)

        // Dropped, but not erased: the outbox keeps what was abandoned.
        let transaction = try #require(context.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(transaction.status == .discarded)

        // And it is never submitted again.
        await store.flush()
        #expect(try await collection.conflicts().isEmpty)
    }

    /// Auto-discard removes user work without anyone asking, so an application
    /// has to be able to say so. A parked-conflict snapshot cannot carry this:
    /// a discarded group is absent from it by construction.
    @Test("A discarded conflict is reported on the discardedConflicts stream")
    func discardIsReported() async throws {
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in throw CollectionNonRetriableError("validation failed") }
        )

        let received = ConflictCollector()
        let stream = await collection.discardedConflicts
        let collecting = Task {
            for await conflict in stream {
                await received.append(conflict)
            }
        }
        defer { collecting.cancel() }

        _ = try? await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Refused")
        }

        try await waitUntil { await received.value().count == 1 }
        let reported = try #require(await received.value().first)
        #expect(reported.error.contains("validation failed"))
        #expect(reported.entries.map(\.key) == ["todo-1"])
        #expect(reported.entries.first?.operation == .create)
    }

    @Test("Quarantine retains the refused intent and its row for inspection")
    func quarantineRetainsIntent() async throws {
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                throw CollectionNonRetriableError("token expired", disposition: .quarantine)
            }
        )

        _ = try? await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Still valid")
        }

        let conflicts = try await collection.conflicts()
        #expect(conflicts.count == 1)
        #expect(conflicts.first?.error.contains("token expired") == true)

        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Still valid")
        #expect(row.collectionSyncState == .conflicted)
    }

    /// The contrast that gives the unknown-baseline test its meaning: with a
    /// baseline the runtime actually observed, the same refused update is
    /// discarded and the row returns to its authoritative value.
    @Test("A refused update with a provable baseline reverts the row")
    func discardRevertsToObservedBaseline() async throws {
        let container = try makeTestContainer()

        let seed = ModelContext(container)
        let synced = TestTodo(id: "todo-1", projectID: "project-a", title: "Authoritative")
        synced.collectionSyncState = .synced
        seed.insert(synced)
        try seed.save()

        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { _ in throw CollectionNonRetriableError("server refused") }
        )

        _ = try? await collection.update("todo-1") { todo in
            todo.title = "Edited"
        }

        #expect(try await collection.conflicts().isEmpty)

        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Authoritative")
        #expect(row.collectionSyncState == .synced)
    }

    /// The property that makes auto-discard safe. Repair reverts a row to its
    /// authoritative baseline, so when that baseline was never observed there is
    /// nothing to revert to, and deleting would be a guess at the user's
    /// expense. The group parks instead, exactly as quarantine would.
    @Test("Discard parks instead of deleting when the baseline is unknown")
    func discardParksWithoutProvableBaseline() async throws {
        let container = try makeTestContainer()

        // A row carried over in a dirty state cannot establish authoritative
        // truth, so intent captured on top of it has an unknown baseline.
        let seed = ModelContext(container)
        let migrated = TestTodo(id: "todo-1", projectID: "project-a", title: "Carried over")
        migrated.collectionSyncState = .error
        seed.insert(migrated)
        try seed.save()

        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onUpdate: { _ in throw CollectionNonRetriableError("server refused") }
        )

        _ = try? await collection.update("todo-1") { todo in
            todo.title = "Edited"
        }

        let conflicts = try await collection.conflicts()
        #expect(conflicts.count == 1)
        #expect(conflicts.first?.repairReadiness == .requiresAuthoritativeRecovery)

        // The user's content is still here, not silently deleted.
        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Edited")
        let transaction = try #require(context.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(transaction.status == .conflicted)
    }
}
