import Foundation
#if canImport(Network)
@preconcurrency import Network
#endif

public enum CollectionConnectivityState: String, Sendable, Hashable, Codable {
    case online
    case offline
}

public protocol CollectionConnectivityMonitoring: Sendable {
    func start()
    func stop()
    func currentState() -> CollectionConnectivityState
    func updates() -> AsyncStream<CollectionConnectivityState>
}

#if canImport(Network)
public final class CollectionNetworkConnectivityMonitor: CollectionConnectivityMonitoring, @unchecked Sendable {
    private let monitor: NWPathMonitor
    private let queue = DispatchQueue(label: "SwiftDataCollection.NetworkConnectivityMonitor")
    private let lock = NSLock()
    private var started = false
    private var state: CollectionConnectivityState = .online
    private var continuations: [UUID: AsyncStream<CollectionConnectivityState>.Continuation] = [:]

    public init(monitor: NWPathMonitor = NWPathMonitor()) {
        self.monitor = monitor
    }

    deinit {
        monitor.cancel()
        finishContinuations()
    }

    public func start() {
        lock.lock()
        guard started == false else {
            lock.unlock()
            return
        }
        started = true
        lock.unlock()

        monitor.pathUpdateHandler = { [weak self] path in
            self?.publish(path.status == .satisfied ? .online : .offline)
        }
        monitor.start(queue: queue)
    }

    public func stop() {
        lock.lock()
        let wasStarted = started
        started = false
        lock.unlock()

        guard wasStarted else { return }
        monitor.cancel()
        finishContinuations()
    }

    public func currentState() -> CollectionConnectivityState {
        lock.lock()
        defer { lock.unlock() }
        return state
    }

    public func updates() -> AsyncStream<CollectionConnectivityState> {
        AsyncStream { continuation in
            let id = UUID()
            lock.lock()
            let current = state
            continuations[id] = continuation
            lock.unlock()

            continuation.yield(current)
            continuation.onTermination = { [weak self] _ in
                self?.removeContinuation(id)
            }
        }
    }

    private func publish(_ newState: CollectionConnectivityState) {
        lock.lock()
        guard state != newState else {
            lock.unlock()
            return
        }
        state = newState
        let activeContinuations = Array(continuations.values)
        lock.unlock()

        for continuation in activeContinuations {
            continuation.yield(newState)
        }
    }

    private func removeContinuation(_ id: UUID) {
        lock.lock()
        continuations.removeValue(forKey: id)
        lock.unlock()
    }

    private func finishContinuations() {
        lock.lock()
        let activeContinuations = Array(continuations.values)
        continuations.removeAll()
        lock.unlock()

        for continuation in activeContinuations {
            continuation.finish()
        }
    }
}
#else
public final class CollectionNetworkConnectivityMonitor: CollectionConnectivityMonitoring, @unchecked Sendable {
    public init() {}
    public func start() {}
    public func stop() {}
    public func currentState() -> CollectionConnectivityState { .online }
    public func updates() -> AsyncStream<CollectionConnectivityState> {
        AsyncStream { continuation in
            continuation.yield(.online)
        }
    }
}
#endif
