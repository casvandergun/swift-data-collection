import Foundation
import SwiftData
import Testing
@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection

/// Ordering is a store-level invariant, not a per-collection one.
///
/// The application records one ordered stream of writes. Before the store-wide
/// lane, each collection scheduled its own outbox, so a child's mutation could
/// reach the server before the parent row it references existed.
@Suite("Store-Wide Dispatch Ordering")
struct CollectionStoreOrderingTests {
    /// Records handler entry and exit so a test can assert that one handler
    /// fully completed before another started.
    actor HandlerLog {
        private var entries: [String] = []
        private var gates: [String: [CheckedContinuation<Void, Never>]] = [:]
        private var openGates: Set<String> = []

        func log(_ entry: String) {
            entries.append(entry)
        }

        func value() -> [String] { entries }

        func enter(_ name: String) async {
            entries.append("\(name):start")
            if openGates.contains(name) == false {
                await withCheckedContinuation { gates[name, default: []].append($0) }
            }
            entries.append("\(name):end")
        }

        func open(_ name: String) {
            openGates.insert(name)
            let waiters = gates.removeValue(forKey: name) ?? []
            waiters.forEach { $0.resume() }
        }
    }

    /// The reported bug: a child insert overtook its parent's POST and the
    /// server rejected it with "parent not found".
    @Test("A later collection's write cannot overtake an earlier collection's in-flight write")
    func laterCollectionWaitsForEarlierInFlightDispatch() async throws {
        let log = HandlerLog()
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)

        let parents = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            dispatchWait: .durablyQueued,
            onInsert: { _ in
                await log.enter("parent")
                return .immediate
            }
        )
        let children = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
            dispatchWait: .durablyQueued,
            onInsert: { _ in
                await log.enter("child")
                return .immediate
            }
        )

        _ = try await parents.insert {
            TestTodo(id: "moment-1", projectID: "project-a", title: "Moment")
        }
        _ = try await children.insert {
            TestEvent(id: "recording-1", title: "Recording", startTime: Date(timeIntervalSince1970: 0))
        }

        // The parent handler is parked, so the child must not have been
        // submitted yet even though its own collection had nothing to wait for.
        // Settle first: asserting the instant the parent starts would also pass
        // against an independent per-collection dispatcher that simply had not
        // got there yet.
        try await waitUntil { await log.value().isEmpty == false }
        try await Task.sleep(nanoseconds: 150_000_000)
        #expect(await log.value() == ["parent:start"])

        await log.open("parent")
        await log.open("child")
        try await waitUntil { await log.value().count == 4 }

        #expect(await log.value() == ["parent:start", "parent:end", "child:start", "child:end"])
    }

    /// Sequence numbers are what order the lane, so they must be allocated
    /// against the store rather than against each collection.
    @Test("Transaction sequence numbers are monotonic across collections")
    func sequenceNumbersAreStoreWide() async throws {
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)

        let todos = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            dispatchWait: .durablyQueued,
            onInsert: { _ in .immediate }
        )
        let events = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
            dispatchWait: .durablyQueued,
            onInsert: { _ in .immediate }
        )

        _ = try await todos.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "First")
        }
        _ = try await events.insert {
            TestEvent(id: "event-1", title: "Second", startTime: Date(timeIntervalSince1970: 0))
        }
        _ = try await todos.insert {
            TestTodo(id: "todo-2", projectID: "project-a", title: "Third")
        }

        let context = ModelContext(container)
        let ordered = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
            .sorted { $0.sequenceNumber < $1.sequenceNumber }
        #expect(ordered.map(\.sequenceNumber) == [0, 1, 2])
        #expect(
            ordered.map(\.modelName) == [
                "SwiftDataCollectionTests.TestTodo",
                "SwiftDataCollectionTests.TestEvent",
                "SwiftDataCollectionTests.TestTodo",
            ]
        )
    }

    /// This package parks permanently refused intent instead of dropping it, so
    /// a terminal transaction sits in the outbox forever. In a single serial
    /// lane that must not stall every other collection.
    @Test("A parked conflict does not block later work in another collection")
    func conflictedTransactionDoesNotBlockTheLane() async throws {
        let log = HandlerLog()
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)

        let refusing = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            dispatchWait: .durablyQueued,
            onInsert: { _ in
                await log.log("refused")
                // Quarantine keeps the refusal in the outbox, which is the
                // case that could stall a single serial lane.
                throw CollectionNonRetriableError(
                    "server refused mutation",
                    disposition: .quarantine
                )
            }
        )
        let healthy = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
            dispatchWait: .durablyQueued,
            onInsert: { _ in
                await log.log("accepted")
                return .immediate
            }
        )

        _ = try await refusing.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Refused")
        }
        _ = try await healthy.insert {
            TestEvent(id: "event-1", title: "Independent", startTime: Date(timeIntervalSince1970: 0))
        }
        await store.flush()

        try await waitUntil { await log.value() == ["refused", "accepted"] }
        #expect(await log.value() == ["refused", "accepted"])

        let context = ModelContext(container)
        let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
            .sorted { $0.sequenceNumber < $1.sequenceNumber }
        // The refusal is retained for inspection, not dropped.
        #expect(transactions.first?.status == .conflicted)
        #expect(transactions.last?.status == .resolved)

        // A later drain must not re-submit the parked conflict.
        await store.flush()
        #expect(await log.value() == ["refused", "accepted"])
    }

    /// Strict FIFO: a retryable head holds the lane rather than letting later
    /// work overtake it, which is what makes parent-before-child hold across a
    /// transient failure.
    @Test("A retryable head holds later collections behind it")
    func retryableHeadHoldsTheLane() async throws {
        let log = HandlerLog()
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(modelContainer: container)

        let failing = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            dispatchWait: .durablyQueued,
            onInsert: { _ in
                await log.log("parent-attempt")
                throw TestTransientError()
            }
        )
        let waiting = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
            dispatchWait: .durablyQueued,
            onInsert: { _ in
                await log.log("child")
                return .immediate
            }
        )

        _ = try await failing.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Transient")
        }
        _ = try await waiting.insert {
            TestEvent(id: "event-1", title: "Behind", startTime: Date(timeIntervalSince1970: 0))
        }
        await store.flush()

        try await waitUntil { await log.value().isEmpty == false }
        try await Task.sleep(nanoseconds: 150_000_000)
        // The head is in backoff, so the child stays queued rather than
        // overtaking it.
        #expect(await log.value() == ["parent-attempt"])

        let context = ModelContext(container)
        let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
            .sorted { $0.sequenceNumber < $1.sequenceNumber }
        #expect(transactions.first?.status == .failed)
        #expect(transactions.last?.status == .pending)
    }

    /// Collections are materialized one at a time at launch, so the earliest
    /// replayed transaction can belong to a collection that has not registered
    /// yet. Dispatching around it would reintroduce the parent/child race on
    /// the restart path.
    @Test("Replay holds for a collection that has not registered yet")
    func replayHoldsForUnregisteredCollections() async throws {
        let location = TestStoreLocation()
        defer { location.cleanup() }
        let log = HandlerLog()

        // Queue a parent and then a child while offline, so both survive to the
        // next launch with the parent holding the earlier sequence number.
        do {
            let offline = TestConnectivityMonitor(initialState: .offline)
            let store = SwiftDataCollectionStore(
                modelContainer: try location.makeContainer(),
                connectivityMonitor: offline
            )
            let parents = try await store.collection(
                TestTodo.self,
                identifier: testTodoIdentifier,
                table: "todos",
                dispatchWait: .durablyQueued,
                onInsert: { _ in .immediate }
            )
            let children = try await store.collection(
                TestEvent.self,
                identifier: testEventIdentifier,
                table: "events",
                dispatchWait: .durablyQueued,
                onInsert: { _ in .immediate }
            )
            _ = try await parents.insert {
                TestTodo(id: "moment-1", projectID: "project-a", title: "Moment")
            }
            _ = try await children.insert {
                TestEvent(id: "recording-1", title: "Recording", startTime: Date(timeIntervalSince1970: 0))
            }
        }

        let relaunched = SwiftDataCollectionStore(modelContainer: try location.makeContainer())

        // The child registers first, as it would if the app happened to build
        // that collection first.
        _ = try await relaunched.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
            dispatchWait: .durablyQueued,
            onInsert: { _ in
                await log.log("child")
                return .immediate
            }
        )
        await relaunched.flush()
        try await Task.sleep(nanoseconds: 150_000_000)
        #expect(await log.value().isEmpty)

        _ = try await relaunched.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            dispatchWait: .durablyQueued,
            onInsert: { _ in
                await log.log("parent")
                return .immediate
            }
        )
        await relaunched.flush()

        try await waitUntil { await log.value().count == 2 }
        #expect(await log.value() == ["parent", "child"])
    }
}

private struct TestTransientError: Error {}
