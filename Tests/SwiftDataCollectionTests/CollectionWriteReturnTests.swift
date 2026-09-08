import Foundation
import SwiftData
import Testing
@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection

/// What a mutation call promises when it returns.
///
/// One contract, unconditionally: the change is durable locally and holds its
/// place in the store-wide order. Reaching the server is the outbox's job.
/// Callers needing the outcome await `CollectionTransaction.wait()`.
@Suite("Write Return Contract")
struct CollectionWriteReturnTests {
    actor HandlerGate {
        private var isReleased = false
        private var waiters: [CheckedContinuation<Void, Never>] = []
        private var invocationCount = 0

        func invocations() -> Int { invocationCount }

        func waitForRelease() async {
            invocationCount += 1
            if isReleased { return }
            await withCheckedContinuation { continuation in
                waiters.append(continuation)
            }
        }

        func release() {
            isReleased = true
            let continuations = waiters
            waiters.removeAll()
            continuations.forEach { $0.resume() }
        }
    }

    /// The local-first contract: a write is never held behind the network, and
    /// under store-wide ordering never behind another collection's backlog
    /// either.
    @Test("A write returns while its handler is still in flight, already durable")
    func writeReturnsBeforeHandlerCompletes() async throws {
        let gate = HandlerGate()
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)

        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                await gate.waitForRelease()
                return .immediate
            }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Draft")
        }

        // The handler is parked, so returning at all proves the write did not
        // wait for it.
        #expect(await transaction.status == .queued)

        // And what it returned is already durable: the optimistic row and its
        // outbox entry survive a crash here.
        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Draft")
        #expect(try context.fetch(FetchDescriptor<PendingCollectionTransaction>()).isEmpty == false)

        await gate.release()
        try await transaction.wait()
        #expect(await gate.invocations() == 1)
    }
}
