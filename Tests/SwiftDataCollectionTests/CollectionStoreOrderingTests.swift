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
            onInsert: { _ in
                await log.enter("parent")
                return .immediate
            }
        )
        let children = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
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
            onInsert: { _ in .immediate }
        )
        let events = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
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
            onInsert: { _ in
                await log.log("parent-attempt")
                throw TestTransientError()
            }
        )
        let waiting = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
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
                onInsert: { _ in .immediate }
            )
            let children = try await store.collection(
                TestEvent.self,
                identifier: testEventIdentifier,
                table: "events",
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
            onInsert: { _ in
                await log.log("parent")
                return .immediate
            }
        )
        await relaunched.flush()

        try await waitUntil { await log.value().count == 2 }
        #expect(await log.value() == ["parent", "child"])
    }

    /// Reconnect brings collections online one at a time. A collection that is
    /// still offline must pause the lane, not be stepped over -- otherwise the
    /// child of a still-offline parent dispatches first.
    @Test("Reconnect replays queued work in store order")
    func reconnectPreservesOrder() async throws {
        let log = HandlerLog()
        let connectivity = TestConnectivityMonitor(initialState: .offline)
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            connectivityMonitor: connectivity
        )

        let parents = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                await log.log("parent")
                return .immediate
            }
        )
        let children = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
            onInsert: { _ in
                await log.log("child")
                return .immediate
            }
        )

        _ = try await parents.insert {
            TestTodo(id: "moment-1", projectID: "project-a", title: "Moment")
        }
        _ = try await children.insert {
            TestEvent(id: "recording-1", title: "Recording", startTime: Date(timeIntervalSince1970: 0))
        }
        #expect(await log.value().isEmpty)

        connectivity.setState(.online)

        try await waitUntil { await log.value().count == 2 }
        #expect(await log.value() == ["parent", "child"])
    }

    /// A process that dies mid-dispatch leaves `sending` on disk. Only the
    /// owning collection's bootstrap can reset it, so until that collection
    /// exists again the record is the only evidence that earlier work is
    /// outstanding.
    @Test("Persisted in-flight work blocks the lane until its collection bootstraps")
    func persistedSendingHoldsTheLane() async throws {
        let location = TestStoreLocation()
        defer { location.cleanup() }
        let log = HandlerLog()

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
                onInsert: { _ in .immediate }
            )
            let children = try await store.collection(
                TestEvent.self,
                identifier: testEventIdentifier,
                table: "events",
                onInsert: { _ in .immediate }
            )
            _ = try await parents.insert {
                TestTodo(id: "moment-1", projectID: "project-a", title: "Moment")
            }
            _ = try await children.insert {
                TestEvent(id: "recording-1", title: "Recording", startTime: Date(timeIntervalSince1970: 0))
            }
        }

        // Simulate dying while the parent's request was in flight.
        let crashContext = ModelContext(try location.makeContainer())
        let parentTransaction = try #require(
            crashContext.fetch(FetchDescriptor<PendingCollectionTransaction>())
                .sorted { $0.sequenceNumber < $1.sequenceNumber }
                .first
        )
        parentTransaction.status = .sending
        try crashContext.save()

        let relaunched = SwiftDataCollectionStore(modelContainer: try location.makeContainer())
        _ = try await relaunched.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
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
            onInsert: { _ in
                await log.log("parent")
                return .immediate
            }
        )
        await relaunched.flush()

        try await waitUntil { await log.value().count == 2 }
        #expect(await log.value() == ["parent", "child"])
    }

    /// Compaction folds a later transaction's changes into an earlier request,
    /// so it may not step over work that has not been submitted -- including
    /// another collection's.
    @Test("Compaction stops at an intervening transaction in another collection")
    func compactionStopsAtInterveningCollection() async throws {
        let log = HandlerLog()
        let connectivity = TestConnectivityMonitor(initialState: .offline)
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            connectivityMonitor: connectivity
        )

        let todos = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { context in
                await log.log("insert:\(titleText(context.mutations[0].modified?["title"]))")
                return .immediate
            },
            onUpdate: { context in
                await log.log("update:\(titleText(context.mutations[0].changes["title"]))")
                return .immediate
            }
        )
        let events = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
            onInsert: { _ in
                await log.log("intervening")
                return .immediate
            }
        )

        _ = try await todos.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "First")
        }
        _ = try await events.insert {
            TestEvent(id: "event-1", title: "Between", startTime: Date(timeIntervalSince1970: 0))
        }
        _ = try await todos.update("todo-1") { todo in
            todo.title = "Revised"
        }

        connectivity.setState(.online)
        try await waitUntil { await log.value().count == 3 }

        // Without the store-wide scan the update folds into the insert, which
        // both submits "Revised" early and drops the third dispatch entirely.
        #expect(await log.value() == ["insert:First", "intervening", "update:Revised"])
    }

    /// The one path that could still reorder: a handler fails, and writing that
    /// failure down fails too. The group stays durably `sending`, owned by
    /// nobody, and stepping past it would let later work overtake it.
    @Test("A dispatch abandoned by a failed failure-write still holds the lane")
    func abandonedSendingDispatchHoldsTheLane() async throws {
        let log = HandlerLog()
        let saver = FailFirstFailureWrite()
        let container = try makeTestContainer()
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            commitSave: { context in try saver.save(context) }
        )

        let parents = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                await log.log("parent-attempt")
                throw TestTransientError()
            }
        )
        let children = try await store.collection(
            TestEvent.self,
            identifier: testEventIdentifier,
            table: "events",
            onInsert: { _ in
                await log.log("child")
                return .immediate
            }
        )

        _ = try await parents.insert {
            TestTodo(id: "moment-1", projectID: "project-a", title: "Moment")
        }
        _ = try await children.insert {
            TestEvent(id: "recording-1", title: "Recording", startTime: Date(timeIntervalSince1970: 0))
        }

        // The parent's failure cannot be recorded, so its dispatch is abandoned
        // with the group left `sending`, owned by nobody.
        await store.flush()
        #expect(saver.failedWrites() == 1)

        // The lane reclaims it rather than stepping past, so the child stays
        // behind it instead of reaching the server first.
        await store.flush()
        try await Task.sleep(nanoseconds: 150_000_000)
        #expect(await log.value() == ["parent-attempt"])

        let recovered = try #require(
            ModelContext(container).fetch(FetchDescriptor<PendingCollectionTransaction>())
                .first { $0.modelName == "SwiftDataCollectionTests.TestTodo" }
        )
        #expect(recovered.status == .failed)
    }
}

/// Fails the first commit that would record a transaction failure, stranding
/// that dispatch in `sending`.
private final class FailFirstFailureWrite: @unchecked Sendable {
    private let lock = NSLock()
    private var failures = 0

    func save(_ context: ModelContext) throws {
        let transactions = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
        let recordsFailure = transactions.contains { $0.status == .failed }
        lock.lock()
        let shouldFail = recordsFailure && failures == 0
        if shouldFail { failures += 1 }
        lock.unlock()
        if shouldFail { throw FailureWriteError() }
        try context.save()
    }

    func failedWrites() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return failures
    }
}

private struct FailureWriteError: Error {}

private struct TestTransientError: Error {}

private func titleText(_ value: CollectionValue?) -> String {
    guard case .string(let text) = value else { return "?" }
    return text
}
