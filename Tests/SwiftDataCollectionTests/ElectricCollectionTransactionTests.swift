@testable import ElectricSwiftDataCollection
import Testing

@Suite("Electric Collection Transactions")
struct ElectricCollectionTransactionTests {
    @Test("Transaction completes waiting authoritative sync")
    func transactionCompletes() async throws {
        let transaction = ElectricCollectionTransaction()

        #expect(await transaction.status == .durablyQueued)

        async let waitForCompletion: Void = transaction.awaitCompletion()
        await transaction.markSending()
        #expect(await transaction.status == .sending)
        await transaction.markAwaitingSync()
        #expect(await transaction.status == .awaitingSync)
        await transaction.complete()

        try await waitForCompletion
        let status = await transaction.status
        #expect(status == .completed)
    }

    @Test("Transaction fails waiting authoritative sync")
    func transactionFails() async {
        enum SampleError: Error { case failed }

        let transaction = ElectricCollectionTransaction()
        async let waitForCompletion: Result<Void, Error> = {
            do {
                try await transaction.awaitCompletion()
                return .success(())
            } catch {
                return .failure(error)
            }
        }()

        await transaction.fail(SampleError.failed)
        let result = await waitForCompletion

        switch result {
        case .success:
            Issue.record("Expected transaction completion to fail")
        case .failure:
            let status = await transaction.status
            if case .failed = status {
                #expect(Bool(true))
            } else {
                Issue.record("Expected failed transaction status")
            }
        }
    }
}
