@testable import ElectricSwiftDataCollection
@testable import SwiftDataCollection
@testable import ElectricSwift
import Foundation
import SwiftData
import Testing
import XCTest

typealias ElectricCollectionStore = SwiftDataCollectionStore
typealias ElectricCollectionTransaction = CollectionTransaction
typealias ElectricCollectionStoreError = SwiftDataCollectionStoreError
typealias ElectricCollectionError = CollectionError
typealias ElectricModelIdentifier = CollectionModelIdentifier
typealias ElectricPendingMutation = PendingCollectionMutation
typealias ElectricPendingTransaction = PendingCollectionTransaction
typealias ElectricCollectionMetadata = CollectionMetadata
typealias ElectricMutationOperation = CollectionMutationOperation
typealias ElectricMutationStatus = PendingMutationStatus
typealias ElectricSyncState = CollectionSyncState
typealias ElectricWriteDebugEvent = CollectionWriteDebugEvent
typealias ElectricWriteTracer = CollectionWriteTracer
typealias ElectricPendingMutationRetryDelaying = PendingMutationRetryDelaying
typealias ElectricForegroundObserverRegistrar = CollectionForegroundObserverRegistrar
typealias ElectricForegroundObserverToken = CollectionForegroundObserverToken

let testElectricCollectionShapeURL = URL(string: "http://localhost:3000/v1/shape")!

extension PendingCollectionTransaction {
    var awaitedTXIDs: [Int64] {
        get { awaitedObservationTokens.compactMap(Int64.init).sorted() }
        set { awaitedObservationTokens = newValue.map(String.init) }
    }
}

extension PendingCollectionMutation {
    var awaitedTXIDs: [Int64] {
        get { awaitedObservationTokens.compactMap(Int64.init).sorted() }
        set { awaitedObservationTokens = newValue.map(String.init) }
    }
}

extension CollectionWriteDebugEvent {
    var awaitedTXIDs: [Int64] {
        awaitedTokens.compactMap(Int64.init)
    }

    var observedTXIDs: [Int64] {
        observedTokens.compactMap(Int64.init)
    }
}

extension SwiftDataCollectionStore {
    func collection<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>(
        _ model: Model.Type,
        identifier: CollectionModelIdentifier<Model, ID>,
        table: String,
        shapeID: String? = nil,
        columns: [String] = [],
        where whereClause: String? = nil,
        replica: ElectricReplica = .default,
        headers: [String: String] = [:],
        extraParameters: [String: String] = [:],
        debugName: String? = nil,
        onInsert: ElectricMutationHandler<Model, ID>? = nil,
        onUpdate: ElectricMutationHandler<Model, ID>? = nil,
        onDelete: ElectricMutationHandler<Model, ID>? = nil
    ) async throws -> SwiftDataCollection<Model, ID> {
        try await collection(
            model,
            identifier: identifier,
            shapeURL: testElectricCollectionShapeURL,
            table: table,
            shapeID: shapeID,
            columns: columns,
            where: whereClause,
            replica: replica,
            headers: headers,
            extraParameters: extraParameters,
            debugName: debugName,
            onInsert: onInsert,
            onUpdate: onUpdate,
            onDelete: onDelete
        )
    }

    func shape<Model: SwiftDataCollectionModel, ID: Hashable & Sendable>(
        _ model: Model.Type,
        identifier: CollectionModelIdentifier<Model, ID>,
        table: String,
        shapeID: String? = nil,
        columns: [String] = [],
        where whereClause: String? = nil,
        replica: ElectricReplica = .default,
        headers: [String: String] = [:],
        extraParameters: [String: String] = [:]
    ) async throws -> ElectricShapeSubscription<Model> {
        try await shape(
            model,
            identifier: identifier,
            shapeURL: testElectricCollectionShapeURL,
            table: table,
            shapeID: shapeID,
            columns: columns,
            where: whereClause,
            replica: replica,
            headers: headers,
            extraParameters: extraParameters
        )
    }
}

@Model
final class TestTodo: SwiftDataCollectionModel {
    var collectionSyncStateRawValue: String
    var collectionPendingMutationCount: Int
    var collectionLastLocalMutationAt: Date?
    var collectionLastServerVersion: String?
    var id: String
    var projectID: String
    var title: String

    init(
        collectionSyncStateRawValue: String = ElectricSyncState.synced.rawValue,
        collectionPendingMutationCount: Int = 0,
        collectionLastLocalMutationAt: Date? = nil,
        collectionLastServerVersion: String? = nil,
        id: String,
        projectID: String,
        title: String
    ) {
        self.collectionSyncStateRawValue = collectionSyncStateRawValue
        self.collectionPendingMutationCount = collectionPendingMutationCount
        self.collectionLastLocalMutationAt = collectionLastLocalMutationAt
        self.collectionLastServerVersion = collectionLastServerVersion
        self.id = id
        self.projectID = projectID
        self.title = title
    }

    convenience init(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws {
        let value = try decoder.decode(TestTodoValue.self, from: collectionRow)
        self.init(
            id: value.id,
            projectID: value.projectID,
            title: value.title
        )
    }

    func apply(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws {
        let value = try decoder.decode(TestTodoValue.self, from: collectionRow)
        self.id = value.id
        self.projectID = value.projectID
        self.title = value.title
    }

    func collectionRow() throws -> CollectionRow {
        testTodoCollectionRow(
            id: id,
            projectID: projectID,
            title: title
        )
    }
}

private struct TestTodoValue: Decodable {
    let id: String
    let projectID: String
    let title: String
}

func makeTestContainer() throws -> ModelContainer {
    let configuration = ModelConfiguration(isStoredInMemoryOnly: true)
    return try ModelContainer(
        for: TestTodo.self,
        ElectricShapeMetadata.self,
        ElectricPendingMutation.self,
        ElectricPendingTransaction.self,
        ElectricCollectionMetadata.self,
        configurations: configuration
    )
}

func makeTestContainer(storeURL: URL) throws -> ModelContainer {
    let configuration = ModelConfiguration(url: storeURL)
    return try ModelContainer(
        for: TestTodo.self,
        ElectricShapeMetadata.self,
        ElectricPendingMutation.self,
        ElectricPendingTransaction.self,
        ElectricCollectionMetadata.self,
        configurations: configuration
    )
}

struct TestStoreLocation: Sendable {
    let storeURL: URL

    init(name: String = UUID().uuidString) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("ElectricSwiftTests", isDirectory: true)
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        self.storeURL = directory.appendingPathComponent("\(name).store")
    }

    func makeContainer() throws -> ModelContainer {
        try makeTestContainer(storeURL: storeURL)
    }

    func cleanup() {
        let fileManager = FileManager.default
        let storePaths = [
            storeURL.path,
            storeURL.path + "-shm",
            storeURL.path + "-wal",
        ]
        for path in storePaths where fileManager.fileExists(atPath: path) {
            try? fileManager.removeItem(atPath: path)
        }
    }
}

func testTodoRow(
    id: String = "todo-1",
    projectID: String = "project-a",
    title: String = "Todo"
) -> ElectricRow {
    [
        "id": .string(id),
        "projectID": .string(projectID),
        "title": .string(title),
    ]
}

func testTodoCollectionRow(
    id: String = "todo-1",
    projectID: String = "project-a",
    title: String = "Todo"
) -> CollectionRow {
    CollectionRow(electricRow: testTodoRow(id: id, projectID: projectID, title: title))
}

func testShapeState(
    handle: String? = "shape-1",
    offset: String = "0_0",
    isLive: Bool = false,
    isUpToDate: Bool = true
) -> ShapeStreamState {
    ShapeStreamState(
        handle: handle,
        offset: offset,
        cursor: nil,
        isLive: isLive,
        isUpToDate: isUpToDate,
        schema: [:],
        lastSyncedAt: Date()
    )
}

func testTodoMessage(
    key: String = "\"public\".\"todos\"/todo-1",
    value: ElectricRow? = nil,
    oldValue: ElectricRow? = nil,
    operation: ElectricOperation? = nil,
    txids: [Int64] = [],
    control: ElectricControl? = nil
) -> ElectricMessage {
    ElectricMessage(
        key: key,
        value: value,
        oldValue: oldValue,
        headers: .init(operation: operation, control: control, txids: txids)
    )
}

func testTodoBatch(
    messages: [ElectricMessage],
    offset: String = "0_0",
    reachedUpToDate: Bool = false,
    handle: String? = "shape-1"
) -> ShapeBatch {
    ShapeBatch(
        messages: messages,
        state: testShapeState(handle: handle, offset: offset, isUpToDate: reachedUpToDate),
        schema: [:],
        reachedUpToDate: reachedUpToDate
    )
}

func makePendingMutation(
    id: UUID = UUID(),
    shapeID: String = "todos",
    targetKey: String = "1",
    operation: ElectricMutationOperation = .update,
    payload: ElectricRow,
    changedFields: Set<String>,
    originalRow: ElectricRow? = nil,
    status: ElectricMutationStatus = .pending,
    metadata: [String: ElectricValue] = [:]
) throws -> ElectricPendingMutation {
    ElectricPendingMutation(
        id: id,
        transactionID: id,
        modelName: String(reflecting: TestTodo.self),
        shapeID: shapeID,
        targetKey: targetKey,
        operation: operation,
        payloadData: try JSONEncoder().encode(CollectionRow(electricRow: payload)),
        changedFieldsData: try JSONEncoder().encode(changedFields),
        originalRowData: try originalRow.map { try JSONEncoder().encode(CollectionRow(electricRow: $0)) },
        metadataData: try JSONEncoder().encode(metadata.mapValues(CollectionValue.init(electricValue:))),
        status: status
    )
}

final class MockURLProtocol: URLProtocol, @unchecked Sendable {
    struct Stub {
        let response: URLResponse
        let data: Data
        let error: Error?
    }

    private static let lock = NSLock()
    private nonisolated(unsafe) static var stubsByKey: [String: [Stub]] = [:]
    private(set) nonisolated(unsafe) static var requests: [URLRequest] = []

    static func reset() {
        lock.lock()
        defer { lock.unlock() }
        stubsByKey.removeAll()
        requests.removeAll()
        ElectricCaches.expiredShapes.clear()
        ElectricTrackers.upToDate.clear()
    }

    static func enqueue(
        response: URLResponse,
        data: Data = Data(),
        error: Error? = nil
    ) {
        lock.lock()
        defer { lock.unlock() }
        let key = stubKey(for: response.url)
        stubsByKey[key, default: []].append(
            Stub(response: response, data: data, error: error)
        )
    }

    static func nextStub(for request: URLRequest) -> Stub? {
        lock.lock()
        defer { lock.unlock() }
        requests.append(request)
        let key = stubKey(for: request.url)
        guard var stubs = stubsByKey[key], stubs.isEmpty == false else {
            return nil
        }
        let stub = stubs.removeFirst()
        if stubs.isEmpty {
            stubsByKey.removeValue(forKey: key)
        } else {
            stubsByKey[key] = stubs
        }
        return stub
    }

    override class func canInit(with request: URLRequest) -> Bool {
        true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let stub = Self.nextStub(for: request) else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }

        if let error = stub.error {
            client?.urlProtocol(self, didFailWithError: error)
            return
        }

        client?.urlProtocol(self, didReceive: stub.response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: stub.data)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}

    private static func stubKey(for url: URL?) -> String {
        guard let url else { return "" }
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url.absoluteString
        }
        components.query = nil
        components.fragment = nil
        return components.url?.absoluteString ?? url.absoluteString
    }
}

func makeMockSession() -> URLSession {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [MockURLProtocol.self]
    return URLSession(configuration: configuration)
}

actor TestShapeTransport: ElectricShapeTransport {
    struct HTTPStub {
        let response: HTTPURLResponse
        let data: Data
        let error: Error?
        let delayMilliseconds: UInt64?
    }

    struct SSEStub {
        let response: HTTPURLResponse
        let chunks: [Data]
        let error: Error?
        let delayMilliseconds: UInt64?
    }

    private var httpStubs: [HTTPStub] = []
    private var sseStubs: [SSEStub] = []
    private var storedRequests: [URLRequest] = []

    func enqueueHTTP(
        response: HTTPURLResponse,
        data: Data = Data(),
        error: Error? = nil,
        delayMilliseconds: UInt64? = nil
    ) {
        httpStubs.append(
            HTTPStub(
                response: response,
                data: data,
                error: error,
                delayMilliseconds: delayMilliseconds
            )
        )
    }

    func enqueueSSE(
        response: HTTPURLResponse,
        chunks: [Data],
        error: Error? = nil,
        delayMilliseconds: UInt64? = nil
    ) {
        sseStubs.append(
            SSEStub(
                response: response,
                chunks: chunks,
                error: error,
                delayMilliseconds: delayMilliseconds
            )
        )
    }

    func requests() -> [URLRequest] {
        storedRequests
    }

    func fetch(_ request: URLRequest) async throws -> ElectricShapeHTTPResponse {
        storedRequests.append(request)
        guard httpStubs.isEmpty == false else {
            throw URLError(.badServerResponse)
        }
        let stub = httpStubs.removeFirst()
        if let error = stub.error {
            throw error
        }
        if let delayMilliseconds = stub.delayMilliseconds {
            do {
                try await Task.sleep(nanoseconds: delayMilliseconds * 1_000_000)
            } catch is CancellationError {
                throw CancellationError()
            }
            try Task.checkCancellation()
        }
        return ElectricShapeHTTPResponse(data: stub.data, response: stub.response)
    }

    func openSSE(_ request: URLRequest) async throws -> ElectricShapeStreamingResponse {
        storedRequests.append(request)
        guard sseStubs.isEmpty == false else {
            throw URLError(.badServerResponse)
        }
        let stub = sseStubs.removeFirst()
        if let error = stub.error {
            throw error
        }

        let chunks = AsyncThrowingStream<Data, Error> { continuation in
            let task = Task {
                do {
                    if let delayMilliseconds = stub.delayMilliseconds {
                        try await Task.sleep(nanoseconds: delayMilliseconds * 1_000_000)
                    }
                    for chunk in stub.chunks {
                        continuation.yield(chunk)
                    }
                    continuation.finish()
                } catch is CancellationError {
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }

        return ElectricShapeStreamingResponse(response: stub.response, chunks: chunks)
    }
}

final class TestRecoveryPolicyRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var recordedSleeps: [TimeInterval] = []
    private let randomValue: Double

    init(randomValue: Double = 1) {
        self.randomValue = randomValue
    }

    func policy() -> ShapeStreamRecoveryPolicy {
        let randomValue = self.randomValue
        return ShapeStreamRecoveryPolicy(
            sleep: { [weak self] delay in
                self?.record(delay)
            },
            randomUnit: {
                randomValue
            }
        )
    }

    private func record(_ delay: TimeInterval) {
        lock.lock()
        recordedSleeps.append(delay)
        lock.unlock()
    }

    func sleeps() -> [TimeInterval] {
        lock.lock()
        defer { lock.unlock() }
        return recordedSleeps
    }
}

func httpResponse(
    url: URL = URL(string: "https://example.com/v1/shape")!,
    statusCode: Int,
    headers: [String: String] = [:]
) -> HTTPURLResponse {
    HTTPURLResponse(url: url, statusCode: statusCode, httpVersion: nil, headerFields: headers)!
}

func jsonData(_ value: some Encodable) throws -> Data {
    try JSONEncoder().encode(value)
}

func waitUntil(
    timeout: TimeInterval = 1,
    interval: UInt64 = 20_000_000,
    _ condition: @escaping () async throws -> Bool
) async throws {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if try await condition() {
            return
        }
        try await Task.sleep(nanoseconds: interval)
    }
    Issue.record("Timed out waiting for condition")
}

let testTodoIdentifier = ElectricModelIdentifier<TestTodo, String>.string(
    get: \.id,
    fetchDescriptor: { id in
        FetchDescriptor(predicate: #Predicate<TestTodo> { $0.id == id })
    }
)

final class TestWriteTraceRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [ElectricWriteDebugEvent] = []

    func tracer() -> ElectricWriteTracer {
        ElectricWriteTracer { [weak self] event in
            self?.record(event)
        }
    }

    var events: [ElectricWriteDebugEvent] {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }

    private func record(_ event: ElectricWriteDebugEvent) {
        lock.lock()
        storage.append(event)
        lock.unlock()
    }
}

final class TestDebugRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [ElectricDebugEvent] = []

    func logger() -> ElectricDebugLogger {
        ElectricDebugLogger { [weak self] event in
            self?.record(event)
        }
    }

    var events: [ElectricDebugEvent] {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }

    private func record(_ event: ElectricDebugEvent) {
        lock.lock()
        storage.append(event)
        lock.unlock()
    }
}

struct TestRetryPolicy: ElectricPendingMutationRetryDelaying {
    let delayInterval: TimeInterval

    func delay(forAttempt attemptCount: Int) -> TimeInterval {
        delayInterval
    }
}

final class TestForegroundObserver: @unchecked Sendable {
    private let lock = NSLock()
    private var handler: (@Sendable () -> Void)?

    func registrar() -> ElectricForegroundObserverRegistrar {
        { [weak self] handler in
            self?.setHandler(handler)
            return [
                ElectricForegroundObserverToken {
                    self?.clearHandler()
                },
            ]
        }
    }

    func fire() {
        lock.lock()
        let handler = self.handler
        lock.unlock()
        handler?()
    }

    private func setHandler(_ handler: @escaping @Sendable () -> Void) {
        lock.lock()
        self.handler = handler
        lock.unlock()
    }

    private func clearHandler() {
        lock.lock()
        handler = nil
        lock.unlock()
    }
}
