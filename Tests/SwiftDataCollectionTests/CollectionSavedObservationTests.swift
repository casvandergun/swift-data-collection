import Foundation
import os
import SwiftData
@testable import SwiftDataCollection
import Testing

@Suite("Saved Collection Evidence", .timeLimit(.minutes(1)))
struct CollectionSavedObservationTests {
    @Test(arguments: [true, false])
    func confirmsAfterBothTheWriteResponseAndSavedAuthoritativeRow(batchFirst: Bool) async throws {
        let container = try makeTestContainer()
        let adapter = SavedObservationAdapter()
        let write = TestWriteResponse()
        let (store, collection) = try await makeCollection(container, adapter: adapter, write: write)
        defer { write.release() }

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Optimistic")
        }
        await write.waitForRequest()

        if batchFirst {
            try await adapter.applyRow(contains: { $0 == "101" })
            #expect(await transaction.status == .sending)
            write.release()
            await collection.flush()
        } else {
            write.release()
            await collection.flush()
            #expect(await transaction.status == .awaiting)
            try await adapter.applyRow(contains: { $0 == "101" })
        }

        try await checkCompletion(transaction, container: container)
        await store.close()
    }

    @Test(arguments: [true, false])
    func failedSaveDoesNotConfirmAWrite(batchFirst: Bool) async throws {
        let container = try makeTestContainer()
        let adapter = SavedObservationAdapter()
        let write = TestWriteResponse()
        let rejectSave = OSAllocatedUnfairLock(initialState: false)
        let (store, collection) = try await makeCollection(
            container,
            adapter: adapter,
            write: write,
            willSave: { _ in
                if rejectSave.withLock({ $0 }) { throw TestSaveError.rejected }
            }
        )
        defer { write.release() }

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Optimistic")
        }
        await write.waitForRequest()
        if !batchFirst {
            write.release()
            await collection.flush()
        }
        rejectSave.withLock { $0 = true }
        await #expect(throws: TestSaveError.rejected) {
            try await adapter.applyRow(contains: { $0 == "101" })
        }
        rejectSave.withLock { $0 = false }
        if batchFirst {
            write.release()
            await collection.flush()
        }

        #expect(await transaction.status == .awaiting)
        let context = ModelContext(container)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Optimistic")

        try await adapter.applyRow(contains: { $0 == "101" })
        try await checkCompletion(transaction, container: container)
        await store.close()
    }

    @Test
    func retriesConfirmationOfCommittedWriteUsingSavedEvidence() async throws {
        let container = try makeTestContainer()
        let adapter = SavedObservationAdapter()
        let write = TestWriteResponse()
        let rejectedSave = OSAllocatedUnfairLock(initialState: false)
        let (store, collection) = try await makeCollection(
            container,
            adapter: adapter,
            write: write,
            willSave: { context in
                guard context.changedModelsArray.contains(where: { model in
                    (model as? TestTodo)?.collectionSyncState == .synced
                }) else { return }
                let firstAttempt = rejectedSave.withLock { rejected in
                    let firstAttempt = !rejected
                    rejected = true
                    return firstAttempt
                }
                if firstAttempt { throw TestSaveError.rejected }
            }
        )
        defer { write.release() }

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Optimistic")
        }
        await write.waitForRequest()
        try await adapter.applyRow(contains: { $0 == "101" })
        write.release()
        await collection.flush()

        // There are no further adapter events. The persisted response and the
        // earlier successful row save are enough to complete the local retry.
        try await waitUntil(timeout: 3) { await transaction.status == .completed }
        #expect(rejectedSave.withLock { $0 })
        #expect(write.attempts.withLock { $0 } == 1)
        try await checkCompletion(transaction, container: container)
        await store.close()
    }

    @Test
    func progressCanConfirmAnAlreadySavedResult() async throws {
        let container = try makeTestContainer()
        let adapter = SavedObservationAdapter()
        let write = TestWriteResponse()
        let (store, collection) = try await makeCollection(container, adapter: adapter, write: write)
        defer { write.release() }

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Optimistic")
        }
        await write.waitForRequest()
        write.release()
        await collection.flush()

        try await adapter.applyRow(contains: { _ in false })
        #expect(await transaction.status == .awaiting)
        try await adapter.applyProgress(contains: { $0 == "101" })
        try await checkCompletion(transaction, container: container)
        await store.close()
    }

    private func makeCollection(
        _ container: ModelContainer,
        adapter: SavedObservationAdapter,
        write: TestWriteResponse,
        willSave: CollectionWillSaveHandler? = nil
    ) async throws -> (SwiftDataCollectionStore, SwiftDataCollection<TestTodo, String>) {
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            willSave: willSave,
            connectivityMonitor: TestConnectivityMonitor()
        )
        let collection = try await store.collection(
            TestTodo.self,
            options: CollectionOptions(
                debugName: "Saved evidence",
                identifier: testTodoIdentifier,
                adapter: CollectionAdapter(sourceID: "saved-evidence") { context in
                    await adapter.configure(context)
                    return adapter
                },
                onInsert: { _ in await write.submit() }
            )
        )
        return (store, collection)
    }

    private func checkCompletion(_ transaction: CollectionTransaction, container: ModelContainer) async throws {
        try #require(await transaction.status == .completed)
        try await transaction.wait()
        let id = await transaction.id
        let context = ModelContext(container)
        let records = try context.fetch(FetchDescriptor<PendingCollectionTransaction>())
        #expect(records.first(where: { $0.id == id })?.status == .resolved)
        let mutations = try context.fetch(FetchDescriptor<PendingCollectionMutation>())
        let mutation = try #require(mutations.first { $0.transactionID == id })
        #expect(mutation.status == .resolved)
        let row = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(row.title == "Normalized by server")
        #expect(row.collectionSyncState == .synced)
    }
}

private actor SavedObservationAdapter: CollectionAdapterRuntime {
    private var context: CollectionAdapterContext<TestTodo, String>?

    func start() {}
    func stop() {}
    func refresh() {}

    func configure(_ context: CollectionAdapterContext<TestTodo, String>) {
        self.context = context
    }

    func applyRow(contains: @escaping CollectionObservation) async throws {
        let context = try #require(context)
        try await context.applyAndConfirm(contains) { modelContext in
            let materializer = CollectionMaterializer(
                context: modelContext,
                collectionID: context.collectionID,
                modelName: String(reflecting: TestTodo.self),
                identifier: context.identifier,
                rowDecoder: context.rowDecoder
            )
            try materializer.apply(.replacement(testTodoCollectionRow(title: "Normalized by server")), for: "todo-1")
            try materializer.materialize(key: "todo-1")
        }
    }

    func applyProgress(contains: @escaping CollectionObservation) async throws {
        let context = try #require(context)
        try await context.applyAndConfirm(contains) { _ in }
    }
}

private struct TestWriteResponse: Sendable {
    private let request = AsyncStream<Void>.makeStream()
    private let response = AsyncStream<Void>.makeStream()
    let attempts = OSAllocatedUnfairLock(initialState: 0)

    func submit() async -> CollectionMutationCompletion {
        attempts.withLock { $0 += 1 }
        request.continuation.finish()
        var iterator = response.stream.makeAsyncIterator()
        _ = await iterator.next()
        return .awaitTokens(["101"])
    }

    func waitForRequest() async {
        var iterator = request.stream.makeAsyncIterator()
        _ = await iterator.next()
    }

    func release() {
        response.continuation.finish()
    }
}

private enum TestSaveError: Error {
    case rejected
}
