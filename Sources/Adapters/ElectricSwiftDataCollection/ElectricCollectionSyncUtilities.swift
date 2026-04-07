import ElectricSwift
import Foundation

public enum ElectricCollectionSyncUtilityError: Error, Sendable {
    case timeout
}

public actor ElectricCollectionSyncUtilities {
    private struct MatchWaiter {
        let predicate: @Sendable (ElectricMessage) -> Bool
        let continuation: CheckedContinuation<Void, Error>
        let timeoutTask: Task<Void, Never>
    }

    private var matchWaiters: [UUID: MatchWaiter] = [:]

    public init() {}

    public func awaitTxID(
        _ txid: Int64,
        timeout: Duration = .seconds(5)
    ) async throws {
        try await awaitMatch(timeout: timeout) { message in
            message.headers.txids?.contains(txid) == true
        }
    }

    public func awaitMatch(
        timeout: Duration = .seconds(3),
        _ predicate: @escaping @Sendable (ElectricMessage) -> Bool
    ) async throws {
        let id = UUID()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let timeoutTask = Task {
                    do {
                        try await Task.sleep(for: timeout)
                    } catch {
                        return
                    }
                    self.resumeWaiter(
                        id,
                        throwing: ElectricCollectionSyncUtilityError.timeout
                    )
                }
                matchWaiters[id] = MatchWaiter(
                    predicate: predicate,
                    continuation: continuation,
                    timeoutTask: timeoutTask
                )
            }
        } onCancel: {
            Task {
                await self.resumeWaiter(id, throwing: CancellationError())
            }
        }
    }

    package func observeAppliedBatch(_ batch: ShapeBatch) {
        guard matchWaiters.isEmpty == false else { return }

        let matchedIDs = matchWaiters.compactMap { id, waiter in
            batch.messages.contains(where: waiter.predicate) ? id : nil
        }

        for id in matchedIDs {
            guard let waiter = matchWaiters.removeValue(forKey: id) else { continue }
            waiter.timeoutTask.cancel()
            waiter.continuation.resume()
        }
    }

    private func resumeWaiter(_ id: UUID, throwing error: Error) {
        guard let waiter = matchWaiters.removeValue(forKey: id) else { return }
        waiter.timeoutTask.cancel()
        waiter.continuation.resume(throwing: error)
    }
}
