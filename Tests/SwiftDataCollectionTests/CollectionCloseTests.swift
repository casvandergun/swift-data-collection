@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection
import Foundation
import SwiftData
import os
import Testing

@Suite("Terminal store close", .timeLimit(.minutes(1)))
struct CollectionCloseTests {
    @Test func closeReleasesObserversAndRejectsNewRegistrations() async throws {
        let container = try makeTestContainer()
        let canceled = OSAllocatedUnfairLock(initialState: 0)
        var store: SwiftDataCollectionStore? = SwiftDataCollectionStore(modelContainer: container,
            foregroundObserverRegistrar: { _ in
                [CollectionForegroundObserverToken { canceled.withLock { $0 += 1 } }]
            }, connectivityMonitor: TestConnectivityMonitor())
        weak let weakStore = store
        _ = try await store?.collection(TestTodo.self, identifier: testTodoIdentifier, table: "todos")
        await store?.close()
        await store?.close()
        #expect(canceled.withLock { $0 } == 1)
        await #expect(throws: SwiftDataCollectionStoreError.self) {
            _ = try await store?.collection(TestTodo.self, identifier: testTodoIdentifier, table: "todos")
        }
        store = nil
        #expect(weakStore == nil)
    }

    @Test func queuedWorkSurvivesCloseAndResumesOnlyOnANewStore() async throws {
        let container = try makeTestContainer()
        let offline = TestConnectivityMonitor(initialState: .offline)
        let recorder = CollectionConnectivityTests.HandlerRecorder()
        let store = SwiftDataCollectionStore(modelContainer: container, connectivityMonitor: offline)
        let collection = try await store.collection(TestTodo.self, identifier: testTodoIdentifier, table: "todos",
            onInsert: { _ in
                _ = await recorder.record()
                return .immediate
            })
        let transaction = try await collection.insert { TestTodo(id: "kept", projectID: "p", title: "Keep me") }
        let waiting = Task { try await transaction.wait() }
        await store.close()
        await #expect(throws: CancellationError.self) { try await waiting.value }
        await #expect(throws: CancellationError.self) { try await transaction.wait() }
        offline.setState(.online)
        await store.flush()
        #expect(await recorder.value() == 0)
        let context = ModelContext(container)
        #expect(try context.fetchCount(FetchDescriptor<TestTodo>()) == 1)
        #expect(try context.fetch(FetchDescriptor<PendingCollectionTransaction>()).first?.status == .pending)

        let next = SwiftDataCollectionStore(modelContainer: container, connectivityMonitor: TestConnectivityMonitor())
        _ = try await next.collection(TestTodo.self, identifier: testTodoIdentifier, table: "todos",
            onInsert: { _ in
                _ = await recorder.record()
                return .immediate
            })
        await next.flush()
        #expect(await recorder.value() == 1)
        await next.close()
    }

    @Test func closeDrainsAnActiveHandlerAndLeavesTheNextWriteQueued() async throws {
        let container = try makeTestContainer()
        let gate = HandlerGate()
        let store = SwiftDataCollectionStore(modelContainer: container, connectivityMonitor: TestConnectivityMonitor())
        let collection = try await store.collection(TestTodo.self, identifier: testTodoIdentifier, table: "todos",
            onInsert: { _ in
                await gate.hold()
                return .immediate
            })
        _ = try await collection.insert { TestTodo(id: "first", projectID: "p", title: "First") }
        await gate.waitUntilEntered()
        _ = try await collection.insert { TestTodo(id: "second", projectID: "p", title: "Second") }
        let closing = Task { await store.close() }
        while !(await store.storeIsClosed) { await Task.yield() }
        await gate.release()
        await closing.value
        #expect(await gate.calls == 1)
        let context = ModelContext(container)
        let pending = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
            .filter { $0.status == .pending }
        #expect(pending.count == 1)
        #expect(try context.fetchCount(FetchDescriptor<TestTodo>()) == 2)
    }

    private actor HandlerGate {
        private(set) var calls = 0
        private var entry: CheckedContinuation<Void, Never>?
        private var releaseContinuation: CheckedContinuation<Void, Never>?
        func hold() async {
            calls += 1
            entry?.resume()
            entry = nil
            await withCheckedContinuation { releaseContinuation = $0 }
        }
        func waitUntilEntered() async {
            if calls > 0 { return }
            await withCheckedContinuation { entry = $0 }
        }
        func release() {
            releaseContinuation?.resume()
            releaseContinuation = nil
        }
    }
}
