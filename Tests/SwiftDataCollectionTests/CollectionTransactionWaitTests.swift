import Foundation
import SwiftData
import Testing
@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection

/// `wait()` is the one place a caller asks for the final outcome, so it has to
/// mean the outcome and nothing weaker.
@Suite("Transaction Wait Contract")
struct CollectionTransactionWaitTests {
    actor AttemptCounter {
        private var count = 0
        func recordAndDecide() -> Bool {
            count += 1
            return count == 1
        }
        func value() -> Int { count }
    }

    /// A retryable error is not an outcome. The outbox is still working, so a
    /// caller asking for the final result must keep waiting across the retry
    /// rather than be handed a failure the runtime is recovering from.
    @Test("Wait survives a retryable failure and returns when the retry succeeds")
    func waitSurvivesRetryableFailure() async throws {
        let attempts = AttemptCounter()
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            retrySleep: { _ in await Task.yield() }
        )
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                if await attempts.recordAndDecide() {
                    throw TransientError()
                }
                return .immediate
            }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Eventually accepted")
        }

        // Before the fix this threw on the first attempt, and the handle was
        // discarded -- so the retry that succeeded completed a different
        // transaction object and this caller waited on an orphan forever.
        try await transaction.wait()

        #expect(await attempts.value() == 2)
        #expect(await transaction.status == .completed)

        let context = ModelContext(container)
        let persisted = try #require(context.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(persisted.status == .resolved)
    }

    @Test("A permanent refusal settles the wait by throwing")
    func waitThrowsOnPermanentRefusal() async throws {
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                throw CollectionNonRetriableError("validation failed", disposition: .quarantine)
            }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Refused")
        }

        await #expect(throws: (any Error).self) {
            try await transaction.wait()
        }
    }

    /// Cancelling the wait abandons the caller's interest, not the mutation.
    /// The write is already durable and the outbox still owns it.
    @Test("Cancelling a wait leaves the durable mutation alone")
    func cancellingWaitDoesNotCancelTheMutation() async throws {
        let gate = HandlerRelease()
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                await gate.hold()
                return .immediate
            }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Slow")
        }

        let waiter = Task { try await transaction.wait() }
        try await waitUntil { await gate.isHeld() }
        waiter.cancel()

        await #expect(throws: CancellationError.self) {
            try await waiter.value
        }

        // The mutation is untouched: releasing the handler still completes it.
        await gate.release()
        try await waitUntil {
            let context = ModelContext(container)
            return try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
                .first?.status == .resolved
        }
    }
}

private struct TransientError: Error {}

private actor HandlerRelease {
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var open = false
    private var held = false

    func isHeld() -> Bool { held }

    func hold() async {
        held = true
        if open { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func release() {
        open = true
        waiters.forEach { $0.resume() }
        waiters.removeAll()
    }
}
