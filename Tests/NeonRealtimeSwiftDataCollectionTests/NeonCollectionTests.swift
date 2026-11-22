import Foundation
import os
@testable import NeonRealtimeSwift
@testable import NeonRealtimeSwiftDataCollection
import SwiftData
@testable import SwiftDataCollection
import Testing

@Suite("Neon Realtime Collection", .timeLimit(.minutes(1)))
struct NeonCollectionTests {
    @Test
    func installsBufferedBatchesAndReplacesOnlyOwnedRowsOnReset() async throws {
        let container = try makeNeonTestContainer()
        try seedLocalRows(container)
        let stream = AsyncThrowingStream<NeonRealtimeEvent, Error>.makeStream()
        defer { stream.continuation.finish() }
        let store = SwiftDataCollectionStore(modelContainer: container)
        let collection = try await store.collection(
            NeonTestTodo.self,
            options: CollectionOptions(
                debugName: "Neon fixture",
                identifier: neonTestIdentifier,
                adapter: CollectionAdapter(sourceID: "neon:fixture") { context in
                    NeonCollectionRuntime(
                        context: context,
                        modelName: String(reflecting: NeonTestTodo.self),
                        subscription: RawLiveQuerySubscription(
                            loadQuery: { SealedLiveQuery(capability: "fixture", queryFingerprint: "query", expiresAt: 1) },
                            subscribe: { _ in stream.stream }
                        ),
                        ownedRows: neonTestScope,
                        decodeRow: decodeNeonTestRow
                    )
                },
                onInsert: nil as CollectionMutationHandler<NeonTestTodo, String>?
            )
        )
        await collection.start()
        stream.continuation.yield(.snapshot(NeonSnapshot(
            columns: testColumns,
            rows: [rawRow("opaque-old", id: "1", title: "Before replay")],
            visibility: NeonTransactionVisibility(xmin: 10, xmax: 20, inProgress: []),
            bufferedBatches: [
                NeonChangeBatch(
                    changes: [.upsert(rawRow("opaque-old", id: "1", title: "After replay"))],
                    transactionIDs: ["20"],
                    lsn: "1/1"
                ),
            ]
        )))
        try await eventually {
            try titles(in: container) == [
                "1": "After replay", "staged": "Unsent local draft", "other": "Out of scope",
            ]
        }
        stream.continuation.yield(.resetRequired)
        stream.continuation.yield(.snapshot(NeonSnapshot(
            columns: testColumns,
            rows: [rawRow("opaque-new", id: "1", title: "After reset")],
            visibility: NeonTransactionVisibility(xmin: 20, xmax: 25, inProgress: []),
            bufferedBatches: []
        )))
        try await eventually {
            try titles(in: container)["1"] == "After reset"
        }
        stream.continuation.yield(.changes(NeonChangeBatch(
            changes: [.remove(rowID: "opaque-new")],
            transactionIDs: ["25"],
            lsn: "1/2"
        )))
        try await eventually {
            try titles(in: container) == ["staged": "Unsent local draft", "other": "Out of scope"]
        }
        let modelContext = ModelContext(container)
        #expect(try modelContext.fetch(neonTestIdentifier.fetchDescriptor(for: "staged")).first?.collectionSyncState == .stagedCreate)
        await store.close()
    }

    @Test
    func reconnectConfirmsCommittedWriteAfterCancellationAndTransientSealFailure() async throws {
        let container = try makeNeonTestContainer()
        let initial = AsyncThrowingStream<NeonRealtimeEvent, Error>.makeStream()
        let replacement = AsyncThrowingStream<NeonRealtimeEvent, Error>.makeStream()
        let reconnected = AsyncStream<SealedLiveQuery>.makeStream()
        defer {
            initial.continuation.finish()
            replacement.continuation.finish()
            reconnected.continuation.finish()
        }
        let loads = OSAllocatedUnfairLock(initialState: 0)
        let writes = OSAllocatedUnfairLock(initialState: 0)
        let delays = OSAllocatedUnfairLock(initialState: [Duration]())
        let loadQuery: @Sendable () throws -> SealedLiveQuery = {
            let attempt = loads.withLock { count in
                count += 1
                return count
            }
            if attempt == 2 {
                throw NeonRealtimeSubscriptionError.sealHTTPStatus(503)
            }
            return SealedLiveQuery(capability: "cap-\(attempt)", queryFingerprint: "query", expiresAt: 1)
        }
        let options = neonCollectionOptions(
            scopeID: "fixture",
            identifier: neonTestIdentifier,
            client: RealtimeClient(url: try #require(URL(string: "wss://example.invalid"))),
            loadQuery: loadQuery,
            ownedRows: neonTestScope,
            decodeRow: decodeNeonTestRow,
            onUpdate: { _ in
                writes.withLock { $0 += 1 }
                return .committed(txid: "25")
            }
        )
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            connectivityMonitor: OnlineNeonTestMonitor()
        )
        let collection = try await store.collection(
            NeonTestTodo.self,
            options: CollectionOptions(
                debugName: options.debugName,
                identifier: options.identifier,
                adapter: CollectionAdapter(sourceID: options.adapter.sourceID) { context in
                    NeonCollectionRuntime(
                        context: context,
                        modelName: options.modelName,
                        subscription: RawLiveQuerySubscription(
                            loadQuery: loadQuery,
                            subscribe: { query in
                                if query.capability == "cap-1" { return initial.stream }
                                reconnected.continuation.yield(query)
                                return replacement.stream
                            },
                            sleepBeforeRetry: { duration in
                                delays.withLock { $0.append(duration) }
                                try Task.checkCancellation()
                            }
                        ),
                        ownedRows: neonTestScope,
                        decodeRow: decodeNeonTestRow
                    )
                },
                onUpdate: options.onUpdate
            )
        )

        await collection.start()
        initial.continuation.yield(.snapshot(NeonSnapshot(
            columns: testColumns,
            rows: [rawRow("first", id: "1", title: "Before write")],
            visibility: NeonTransactionVisibility(xmin: 10, xmax: 20, inProgress: []),
            bufferedBatches: []
        )))
        try await eventually { try titles(in: container)["1"] == "Before write" }

        let transaction = try await collection.update("1") { $0.title = "Optimistic" }
        await collection.flush()
        try #require(await transaction.status == .awaiting)
        let waiter = Task { try await transaction.wait() }
        defer { waiter.cancel() }
        initial.continuation.finish(throwing: NeonRealtimeError.server(
            code: "upstream_cancelled",
            message: "Subscription cancelled",
            sqlState: nil,
            retryAfterMilliseconds: 4_500
        ))

        var reconnect = reconnected.stream.makeAsyncIterator()
        let query = try #require(await reconnect.next())
        #expect(query.capability == "cap-3")
        #expect(delays.withLock { $0 } == [.milliseconds(4_500), .seconds(2)])
        #expect(await transaction.status == .awaiting)
        #expect(try titles(in: container)["1"] == "Optimistic")
        replacement.continuation.yield(.snapshot(NeonSnapshot(
            columns: testColumns,
            rows: [rawRow("second", id: "1", title: "Saved by server")],
            visibility: NeonTransactionVisibility(xmin: 20, xmax: 30, inProgress: []),
            bufferedBatches: []
        )))
        try await eventually { try titles(in: container)["1"] == "Saved by server" }
        try await waiter.value
        #expect(await transaction.status == .completed)
        let rowContext = ModelContext(container)
        let row = try #require(rowContext.fetch(neonTestIdentifier.fetchDescriptor(for: "1")).first)
        #expect(row.collectionSyncState == .synced)
        #expect(row.collectionPendingMutationCount == 0)
        #expect(writes.withLock { $0 } == 1)
        await store.close()
    }

    @available(macOS 26, iOS 26, *)
    @Test
    func stopSupersedesRefreshWhileReaderIsApplyingASnapshot() async throws {
        let events = AsyncThrowingStream<NeonRealtimeEvent, Error>.makeStream()
        let applying = AsyncStream<CheckedContinuation<Void, Never>>.makeStream()
        let loads = OSAllocatedUnfairLock(initialState: 0)
        let subscription = RawLiveQuerySubscription(
            loadQuery: {
                loads.withLock { $0 += 1 }
                return SealedLiveQuery(capability: "fixture", queryFingerprint: "query", expiresAt: 1)
            },
            subscribe: { _ in events.stream }
        )
        let context = CollectionAdapterContext(
            modelContainer: try makeNeonTestContainer(),
            collectionID: "fixture",
            sourceID: "neon:fixture",
            debugName: "Neon lifecycle fixture",
            identifier: neonTestIdentifier,
            rowDecoder: CollectionRowDecoder(),
            debugLogger: .disabled,
            tracer: .disabled,
            writeGate: CollectionWriteGate(),
            willSave: nil,
            onApply: nil,
            applyAndConfirm: { _, _ in
                await withCheckedContinuation { continuation in
                    applying.continuation.yield(continuation)
                }
            },
            reportApplied: { _, _, _ in },
            reportRefreshCompleted: { _ in },
            reportError: { _ in }
        )
        let runtime = NeonCollectionRuntime(
            context: context,
            modelName: String(reflecting: NeonTestTodo.self),
            subscription: subscription,
            ownedRows: neonTestScope,
            decodeRow: decodeNeonTestRow
        )
        defer {
            applying.continuation.finish()
            events.continuation.finish()
        }

        await runtime.start()
        events.continuation.yield(.snapshot(NeonSnapshot(
            columns: testColumns,
            rows: [],
            visibility: NeonTransactionVisibility(xmin: 10, xmax: 20, inProgress: []),
            bufferedBatches: []
        )))
        var applications = applying.stream.makeAsyncIterator()
        let resumeApply = try #require(await applications.next())

        await refreshThenStop(runtime, resumeApply: resumeApply)
        // The reader is an asynchronous task. Give an erroneous restart time
        // to invoke the query loader before checking that only one run opened.
        try await Task.sleep(for: .milliseconds(100))
        #expect(loads.withLock { $0 } == 1)
        #expect(await subscription.state == .stopped)
        await runtime.stop()
    }

    @available(macOS 26, iOS 26, *)
    private func refreshThenStop(
        _ runtime: isolated NeonCollectionRuntime<NeonTestTodo, String>,
        resumeApply: CheckedContinuation<Void, Never>
    ) async {
        let refresh = Task.immediate { await runtime.refresh() }
        let stop = Task.immediate { await runtime.stop() }
        resumeApply.resume()
        await refresh.value
        await stop.value
    }

    enum NoOpRestoreFailure: CaseIterable {
        case none
        case once
        case untilRestart
    }

    @Test(arguments: NoOpRestoreFailure.allCases)
    func unchangedWriteRestoresSavedAuthoritativeRow(failure: NoOpRestoreFailure) async throws {
        let container = try makeNeonTestContainer()
        let key = "1"
        let savedTitle = "Saved by server"
        let optimisticTitle = "Optimistic update"
        let seedContext = ModelContext(container)
        seedContext.insert(NeonTestTodo(id: key, scope: "live", title: savedTitle))
        try seedContext.save()

        let outboundCalls = OSAllocatedUnfairLock(initialState: 0)
        let restoreAttempts = OSAllocatedUnfairLock(initialState: 0)
        let response = AsyncStream<Void>.makeStream()
        defer { response.continuation.finish() }
        let options = neonCollectionOptions(
            scopeID: "fixture",
            identifier: neonTestIdentifier,
            client: RealtimeClient(url: URL(string: "wss://example.invalid")!),
            loadQuery: { throw TestFixtureError.unexpectedQueryLoad },
            ownedRows: neonTestScope,
            decodeRow: decodeNeonTestRow,
            onUpdate: { _ in
                outboundCalls.withLock { $0 += 1 }
                for await _ in response.stream {}
                return .unchanged
            }
        )
        let store = SwiftDataCollectionStore(
            modelContainer: container,
            willSave: { context in
                guard context.changedModelsArray.contains(where: { model in
                    guard let todo = model as? NeonTestTodo else { return false }
                    return todo.id == key && todo.title == savedTitle && todo.collectionSyncState == .synced
                }) else { return }
                let attempt = restoreAttempts.withLock { attempts in
                    attempts += 1
                    return attempts
                }
                if failure == .untilRestart || (failure == .once && attempt == 1) {
                    throw TestFixtureError.restoreRejected
                }
            },
            connectivityMonitor: OnlineNeonTestMonitor()
        )
        let collection = try await store.collection(NeonTestTodo.self, options: options)
        let transaction = try await collection.update(key) { $0.title = optimisticTitle }
        #expect(try titles(in: container)[key] == optimisticTitle)

        response.continuation.finish()
        await collection.flush()
        if failure == .untilRestart {
            #expect(await transaction.status == .awaiting)
            #expect(try titles(in: container)[key] == optimisticTitle)
            await store.close()

            let reopened = SwiftDataCollectionStore(
                modelContainer: container,
                connectivityMonitor: OnlineNeonTestMonitor()
            )
            _ = try await reopened.collection(NeonTestTodo.self, options: options)
            await reopened.close()
        } else {
            try await eventually { try titles(in: container)[key] == savedTitle }
            try await transaction.wait()
            #expect(await transaction.status == .completed)
            #expect(restoreAttempts.withLock { $0 } == (failure == .once ? 2 : 1))
            await store.close()
        }

        #expect(outboundCalls.withLock { $0 } == 1)
        let context = ModelContext(container)
        let row = try #require(context.fetch(neonTestIdentifier.fetchDescriptor(for: key)).first)
        #expect(row.title == savedTitle)
        #expect(row.collectionSyncState == .synced)
        #expect(row.collectionPendingMutationCount == 0)
        let transactionID = await transaction.id
        let record = try #require(context.fetch(FetchDescriptor<PendingCollectionTransaction>()).first {
            $0.id == transactionID
        })
        #expect(record.status == .resolved)
        let mutation = try #require(context.fetch(FetchDescriptor<PendingCollectionMutation>()).first {
            $0.transactionID == transactionID
        })
        #expect(mutation.status == .resolved)
    }

    @Test
    func visibilityKeepsEarlierProofAndCompactsExactIDs() throws {
        var observation = NeonCollectionObservation()
        try observation.include(["39", "50"])
        observation.include(NeonTransactionVisibility(xmin: 30, xmax: 45, inProgress: [31, 39, 42]))
        #expect(observation.contains("39"))
        #expect(!observation.contains("31"))
        #expect(observation.contains("44"))
        #expect(observation.contains("50"))

        observation.include(NeonTransactionVisibility(xmin: 32, xmax: 35, inProgress: []))
        #expect(observation.contains("31"))
        #expect(observation.contains("44"))
        #expect(!observation.contains("42"))
        observation.include(NeonTransactionVisibility(xmin: 50, xmax: 60, inProgress: []))
        #expect(observation.contains("50"))
        #expect(!observation.contains("60"))
    }

    private func seedLocalRows(_ container: ModelContainer) throws {
        let context = ModelContext(container)
        context.insert(NeonTestTodo(id: "cached", scope: "live", title: "Cached row"))
        let staged = NeonTestTodo(id: "staged", scope: "live", title: "Unsent local draft")
        staged.collectionSyncState = .stagedCreate
        context.insert(staged)
        context.insert(NeonTestTodo(id: "other", scope: "other", title: "Out of scope"))
        try context.save()
    }
}

private let testColumns = ["id", "title"].map {
    NeonColumn(name: $0, typeOID: 25, typeModifier: -1, codec: .postgresText)
}

private func rawRow(_ rowID: String, id: String, title: String) -> NeonRow {
    NeonRow(rowID: rowID, values: [.text(id), .text(title)])
}

@Model
final class NeonTestTodo: SwiftDataCollectionModel {
    @Attribute(.unique) var id: String
    var scope: String
    var title: String
    var collectionSyncState = CollectionSyncState.synced
    var collectionPendingMutationCount: Int = 0

    init(id: String, scope: String, title: String) {
        self.id = id
        self.scope = scope
        self.title = title
    }

    convenience init(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws {
        let row = try decoder.decode(Row.self, from: collectionRow)
        self.init(id: row.id, scope: row.scope, title: row.title)
    }

    func apply(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws {
        let row = try decoder.decode(Row.self, from: collectionRow)
        scope = row.scope
        title = row.title
    }

    func collectionRow() throws -> CollectionRow {
        ["id": .string(id), "scope": .string(scope), "title": .string(title)]
    }

    private struct Row: Decodable {
        let id: String
        let scope: String
        let title: String
    }
}

let neonTestIdentifier = CollectionModelIdentifier<NeonTestTodo, String>.string(
    get: \.id,
    fetchDescriptor: { id in FetchDescriptor(predicate: #Predicate { $0.id == id }) }
)

func neonTestScope() -> FetchDescriptor<NeonTestTodo> {
    FetchDescriptor(predicate: #Predicate { $0.scope == "live" })
}

func makeNeonTestContainer() throws -> ModelContainer {
    let schema = Schema(SwiftDataCollectionSchema.models(including: [NeonTestTodo.self]))
    return try ModelContainer(for: schema, configurations: ModelConfiguration(isStoredInMemoryOnly: true))
}

func decodeNeonTestRow(_ row: NeonRow, _ columns: [NeonColumn]) throws -> CollectionRow {
    guard columns.map(\.name) == ["id", "title"], row.values.count == 2,
          case .text(let id) = row.values[0], case .text(let title) = row.values[1] else {
        throw TestFixtureError.unexpectedRow
    }
    return ["id": .string(id), "scope": .string("live"), "title": .string(title)]
}

func titles(in container: ModelContainer) throws -> [String: String] {
    let context = ModelContext(container)
    return Dictionary(uniqueKeysWithValues: try context.fetch(FetchDescriptor<NeonTestTodo>()).map {
        ($0.id, $0.title)
    })
}

func eventually(
    timeout: Duration = .seconds(3),
    _ check: () throws -> Bool
) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + timeout
    while clock.now < deadline {
        if try check() { return }
        try await Task.sleep(for: .milliseconds(15))
    }
    throw TestFixtureError.waitTimedOut
}

private struct OnlineNeonTestMonitor: CollectionConnectivityMonitoring {
    func start() {}
    func stop() {}
    func currentState() -> CollectionConnectivityState { .online }
    func updates() -> AsyncStream<CollectionConnectivityState> {
        AsyncStream { $0.finish() }
    }
}

enum TestFixtureError: Error {
    case unexpectedRow
    case unexpectedQueryLoad
    case restoreRejected
    case waitTimedOut
}
