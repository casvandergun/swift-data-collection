@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection
import Foundation
import SwiftData
import Testing

@Suite("Collection Connectivity")
struct CollectionConnectivityTests {
    actor HandlerRecorder {
        private var count = 0
        private var failuresRemaining: Int

        init(failuresRemaining: Int = 0) {
            self.failuresRemaining = failuresRemaining
        }

        func recordOrThrow(_ error: Error) throws -> Int {
            count += 1
            if failuresRemaining > 0 {
                failuresRemaining -= 1
                throw error
            }
            return count
        }

        func record() -> Int {
            count += 1
            return count
        }

        func value() -> Int { count }
    }

    @Test("Offline collection queues optimistic mutation without invoking handler")
    func offlineCollectionQueuesMutationUntilReconnect() async throws {
        let container = try makeTestContainer()
        let connectivity = TestConnectivityMonitor(initialState: .offline)
        let recorder = HandlerRecorder()
        let traceRecorder = TestTraceRecorder()
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            diagnostics: traceRecorder.diagnostics(),
            connectivityMonitor: connectivity
        )

        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                _ = await recorder.record()
                return ElectricMutationSubmission(awaitedTXIDs: [101])
            }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Offline insert")
        }

        #expect(await recorder.value() == 0)
        #expect(await collection.status == .offline)

        let offlineContext = ModelContext(container)
        let optimistic = try #require(offlineContext.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(optimistic.collectionSyncState == .pendingCreate)
        #expect(optimistic.collectionPendingMutationCount == 1)

        let pending = try #require(offlineContext.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(pending.status == .pending)

        connectivity.setState(.online)

        try await waitUntil {
            await recorder.value() == 1
        }

        let onlineContext = ModelContext(container)
        let awaiting = try #require(onlineContext.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(awaiting.status == .awaitingSync)
        #expect(awaiting.awaitedObservationTokens == ["101"])
        #expect(traceRecorder.events.contains { $0.kind == .dispatchPausedOffline })
        #expect(traceRecorder.events.contains { $0.kind == .dispatchResumedOnline })
    }

    @Test("Reconnect makes failed retry eligible immediately")
    func reconnectRetriesFailedTransactionImmediately() async throws {
        let container = try makeTestContainer()
        let connectivity = TestConnectivityMonitor(initialState: .online)
        let recorder = HandlerRecorder(failuresRemaining: 1)
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            retryPolicy: TestRetryPolicy(delayInterval: 3600),
            connectivityMonitor: connectivity
        )

        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                _ = try await recorder.recordOrThrow(URLError(.notConnectedToInternet))
                return ElectricMutationSubmission(awaitedTXIDs: [202])
            }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Retry insert")
        }

        #expect(await recorder.value() == 1)
        let failedContext = ModelContext(container)
        let failed = try #require(failedContext.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(failed.status == .failed)
        #expect(failed.nextRetryAt.map { $0 > Date().addingTimeInterval(3000) } == true)

        connectivity.setState(.offline)
        try await waitUntil {
            await collection.status == .offline
        }

        connectivity.setState(.online)

        try await waitUntil {
            await recorder.value() == 2
        }

        let retriedContext = ModelContext(container)
        let retried = try #require(retriedContext.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        #expect(retried.status == .awaitingSync)
        #expect(retried.awaitedObservationTokens == ["202"])
    }

    @Test("Non-retriable handler error marks mutation conflicted")
    func nonRetriableErrorMarksMutationConflicted() async throws {
        let container = try makeTestContainer()
        let recorder = HandlerRecorder()
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                _ = await recorder.record()
                throw CollectionNonRetriableError("validation failed")
            }
        )

        _ = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Invalid insert")
        }
        await store.flush()

        #expect(await recorder.value() == 1)
        let context = ModelContext(container)
        let transaction = try #require(context.fetch(FetchDescriptor<PendingCollectionTransaction>()).first)
        let mutation = try #require(context.fetch(FetchDescriptor<PendingCollectionMutation>()).first)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)

        #expect(transaction.status == .conflicted)
        #expect(transaction.nextRetryAt == nil)
        #expect(mutation.status == .conflicted)
        #expect(mutation.nextRetryAt == nil)
        #expect(row.collectionSyncState == .syncError)
        #expect(row.collectionPendingMutationCount == 1)
    }
}
