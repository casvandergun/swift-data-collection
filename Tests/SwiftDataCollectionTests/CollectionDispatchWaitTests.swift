@testable import ElectricSwiftDataCollection
@testable import ElectricSwift
import SwiftData
import Testing

@Suite("Collection Dispatch Wait")
struct CollectionDispatchWaitTests {
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

    /// The point of `durablyQueued`: a write must not wait on the handler, which
    /// is where the network round trip lives.
    @Test("Durably queued writes return while the handler is still in flight")
    func durablyQueuedWriteReturnsBeforeHandlerCompletes() async throws {
        let gate = HandlerGate()
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            dispatchWait: .durablyQueued,
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
        #expect(await transaction.status == .durablyQueued)

        // The optimistic row and its outbox entry are already durable.
        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Draft")
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty == false)

        await gate.release()
        try await transaction.wait()
        #expect(await gate.invocations() == 1)
    }

    /// The default is unchanged: dispatch has been attempted by the time the
    /// write returns.
    @Test("Dispatch-attempted writes return only after the handler runs")
    func dispatchAttemptedWriteWaitsForHandler() async throws {
        let gate = HandlerGate()
        await gate.release()

        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                await gate.waitForRelease()
                return .immediate
            }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Draft")
        }

        #expect(await gate.invocations() == 1)
    }
}
