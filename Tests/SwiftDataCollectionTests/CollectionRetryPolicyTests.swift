@testable import SwiftDataCollection
import Foundation
import Testing

@Suite("Collection Retry Policy")
struct CollectionRetryPolicyTests {
    @Test("Default retry policy returns finite bounded delays for extreme attempts")
    func defaultRetryPolicyBoundsExtremeAttempts() {
        let policy = CollectionRetryPolicy()

        for attemptCount in [Int.min, -1, 0, 1, Int.max] {
            let delay = policy.delay(forAttempt: attemptCount)

            #expect(delay.isFinite)
            #expect(delay > 0)
            #expect(delay <= CollectionRetryPolicy.maximumDelay)
        }
    }

    @Test("Default retry sleep ignores non-finite and non-positive delays")
    func defaultRetrySleepIgnoresInvalidDelays() async {
        await defaultCollectionRetrySleep(.nan)
        await defaultCollectionRetrySleep(.infinity)
        await defaultCollectionRetrySleep(-1)
        await defaultCollectionRetrySleep(0)
    }

    @Test("Default retry sleep bounds huge finite delays before sleeping")
    func defaultRetrySleepBoundsHugeFiniteDelay() async {
        let task = Task {
            await defaultCollectionRetrySleep(.greatestFiniteMagnitude)
        }

        task.cancel()
        await task.value
    }

    @Test("Transaction attempt recording saturates at Int.max")
    func transactionAttemptRecordingSaturatesAtIntMax() {
        let transaction = PendingCollectionTransaction(
            collectionID: "todos",
            shapeID: "todos",
            modelName: "TestTodo",
            sequenceNumber: 1,
            attemptCount: Int.max
        )

        transaction.recordAttempt()

        #expect(transaction.attemptCount == Int.max)
    }

    @Test("Mutation attempt recording saturates at Int.max")
    func mutationAttemptRecordingSaturatesAtIntMax() {
        let mutation = PendingCollectionMutation(
            transactionID: UUID(),
            modelName: "TestTodo",
            shapeID: "todos",
            targetKey: "todo-1",
            operation: .update,
            payloadData: Data(),
            attemptCount: Int.max
        )

        mutation.recordAttempt()

        #expect(mutation.attemptCount == Int.max)
    }
}
