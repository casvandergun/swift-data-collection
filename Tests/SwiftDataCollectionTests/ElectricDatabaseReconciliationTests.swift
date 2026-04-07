@testable import ElectricSwiftDataCollection
@testable import ElectricSwift
import SwiftData
import Testing

@Suite("Electric Database Reconciliation")
struct ElectricDatabaseReconciliationTests {
    @Test("Transaction completes when awaited txid is observed")
    func completesTransactionFromObservedTXID() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }
        let initialStatus = await transaction.status
        switch initialStatus {
        case .durablyQueued, .sending, .awaitingSync:
            #expect(Bool(true))
        case .completed, .failed:
            Issue.record("Expected transaction to still be in progress before txid reconciliation")
        }

        let context = ModelContext(container)
        let pendingBefore = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pendingBefore.status == .awaitingSync)

        async let waitForCompletion: Void = transaction.awaitCompletion()

        let batch = ShapeBatch(
            messages: [
                ElectricMessage(
                    key: "\"public\".\"todos\"/todo-1",
                    value: testTodoRow(title: "Inserted"),
                    headers: .init(operation: .insert, txids: [101])
                ),
            ],
            state: testShapeState(offset: "4_0"),
            schema: [:],
            reachedUpToDate: false
        )
        let result = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            batch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: batch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: result.resolvedTransactionIDs
        )

        try await waitForCompletion

        let status = await transaction.status
        #expect(status == .completed)

        let pendingAfter = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pendingAfter.status == .resolved)

        let persistedTodo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(persistedTodo.title == "Inserted")
    }

    @Test("Insert without a stable identifier throws before local commit")
    func insertRequiresStableKey() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) }
        )

        do {
            _ = try await collection.insert {
                TestTodo(id: "", projectID: "project-a", title: "Inserted")
            }
            Issue.record("Expected insert without a stable identifier to throw")
        } catch ElectricCollectionError.missingStableIdentifier {
            #expect(Bool(true))
        } catch {
            Issue.record("Expected missingStableIdentifier, got \(error)")
        }

        let context = ModelContext(container)
        #expect(try context.fetch(FetchDescriptor<TestTodo>()).isEmpty)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)
    }

    @Test("Failed dispatch keeps local row and marks sync error")
    func failedDispatchMarksSyncError() async throws {
        enum SampleError: Error { case sendFailed }

        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in throw SampleError.sendFailed }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }

        do {
            try await transaction.awaitCompletion()
            Issue.record("Expected completion wait to fail")
        } catch {
            #expect(Bool(true))
        }

        let context = ModelContext(container)
        let todo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(todo.collectionSyncState == .syncError)

        let pending = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pending.status == .failed)
    }

    @Test("Failed dispatch retries autonomously without manual flush")
    func failedDispatchRetriesAutonomously() async throws {
        enum SampleError: Error { case sendFailed }

        actor AttemptTracker {
            private var count = 0

            func next() -> Int {
                count += 1
                return count
            }
        }

        let tracker = AttemptTracker()
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container,
            retryPolicy: TestRetryPolicy(delayInterval: 0.01)
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in
                if await tracker.next() == 1 {
                    throw SampleError.sendFailed
                }
                return ElectricMutationSubmission(awaitedTXIDs: [707])
            }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }

        do {
            try await transaction.awaitCompletion()
            Issue.record("Expected initial completion wait to fail before autonomous retry")
        } catch {
            #expect(Bool(true))
        }

        let context = ModelContext(container)
        let deadline = Date().addingTimeInterval(1)
        while Date() < deadline {
            let pending = try context.fetch(FetchDescriptor<ElectricPendingMutation>())
            if pending.first?.status == .awaitingSync {
                break
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }

        let retriedMutation = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(retriedMutation.status == .awaitingSync)
        let retriedRow = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(retriedRow.collectionSyncState == .pendingCreate)

        let batch = ShapeBatch(
            messages: [
                ElectricMessage(
                    key: "\"public\".\"todos\"/todo-1",
                    value: testTodoRow(title: "Inserted"),
                    headers: .init(operation: .insert, txids: [707])
                ),
            ],
            state: testShapeState(offset: "7_0"),
            schema: [:],
            reachedUpToDate: false
        )
        let result = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            batch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: batch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: result.resolvedTransactionIDs
        )

        let resolvedDeadline = Date().addingTimeInterval(1)
        while Date() < resolvedDeadline {
            let pending = try context.fetch(FetchDescriptor<ElectricPendingMutation>())
            if pending.first?.status == .resolved {
                break
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }

        let resolvedMutation = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(resolvedMutation.status == .resolved)
    }

    @Test("Exact duplicate raw shape reuses the managed registration")
    func duplicateShapeReuseIsAllowed() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let first = try await database.shape(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )
        let second = try await database.shape(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        #expect(first.shapeID == second.shapeID)
        #expect(first.shape == second.shape)
    }

    @Test("Conflicting managed shapes for the same model throw")
    func conflictingShapesThrow() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        _ = try await database.shape(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        do {
            _ = try await database.shape(
                TestTodo.self,
                identifier: testTodoIdentifier,
                table: "todos",
                where: "project_id = 'a'"
            )
            Issue.record("Expected conflicting shape registration to throw")
        } catch ElectricCollectionStoreError.managedShapeConflict(
            let modelName,
            let existingKind,
            let existingShapeID,
            let requestedKind,
            let requestedShapeID
        ) {
            #expect(modelName == String(reflecting: TestTodo.self))
            #expect(existingKind == .shape)
            #expect(existingShapeID == "todos")
            #expect(requestedKind == .shape)
            #expect(requestedShapeID == "todos:project_id = 'a'")
        } catch {
            Issue.record("Expected managedShapeConflict, got \(error)")
        }
    }

    @Test("Shape then collection for the same model conflicts")
    func shapeThenCollectionConflicts() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        _ = try await database.shape(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        do {
            _ = try await database.collection(
                TestTodo.self,
                identifier: testTodoIdentifier,
                table: "todos"
            )
            Issue.record("Expected collection registration to conflict with existing managed shape")
        } catch ElectricCollectionStoreError.managedShapeConflict(
            _,
            let existingKind,
            _,
            let requestedKind,
            _
        ) {
            #expect(existingKind == .shape)
            #expect(requestedKind == .collection)
        } catch {
            Issue.record("Expected managedShapeConflict, got \(error)")
        }
    }

    @Test("Duplicate collection reuse requires handler-free second registration")
    func duplicateCollectionReuseAndConflictRules() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let first = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            debugName: "primary",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) }
        )
        let second = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        #expect(first.shapeID == second.shapeID)
        #expect(first.debugName == second.debugName)

        do {
            _ = try await database.collection(
                TestTodo.self,
                identifier: testTodoIdentifier,
                table: "todos",
                onDelete: { _ in ElectricMutationSubmission(awaitedTXIDs: [202]) }
            )
            Issue.record("Expected duplicate collection with handlers to conflict")
        } catch ElectricCollectionStoreError.managedShapeConflict(
            _,
            let existingKind,
            _,
            let requestedKind,
            _
        ) {
            #expect(existingKind == .collection)
            #expect(requestedKind == .collection)
        } catch {
            Issue.record("Expected managedShapeConflict, got \(error)")
        }
    }

    @Test("Atomic commit failure leaves no optimistic row or outbox state")
    func commitFailureRollsBackOptimisticChanges() async throws {
        enum SampleError: Error { case commitFailed }

        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container,
            commitSave: { _ in throw SampleError.commitFailed }
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) }
        )

        do {
            _ = try await collection.insert {
                TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
            }
            Issue.record("Expected collection insert to fail when atomic commit save fails")
        } catch SampleError.commitFailed {
            #expect(Bool(true))
        } catch {
            Issue.record("Expected commitFailed error, got \(error)")
        }

        let context = ModelContext(container)
        #expect(try context.fetch(FetchDescriptor<TestTodo>()).isEmpty)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingTransaction>()).isEmpty)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)
    }

    @Test("Deferred optimistic delete keeps row visible until authoritative delete")
    func deferredDeleteRemainsVisibleUntilServerDelete() async throws {
        let container = try makeTestContainer()
        let setupContext = ModelContext(container)
        setupContext.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Existing"))
        try setupContext.save()

        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onDelete: { _ in ElectricMutationSubmission(awaitedTXIDs: [301]) }
        )

        let transaction = try await collection.delete("todo-1")
        let context = ModelContext(container)
        let visibleRow = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(visibleRow.collectionSyncState == .pendingDelete)
        #expect(visibleRow.collectionPendingMutationCount == 1)

        let pendingBefore = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pendingBefore.status == .awaitingSync)
        #expect(pendingBefore.operation == .delete)

        async let waitForCompletion: Void = transaction.awaitCompletion()

        let batch = ShapeBatch(
            messages: [
                ElectricMessage(
                    key: "\"public\".\"todos\"/todo-1",
                    oldValue: testTodoRow(title: "Existing"),
                    headers: .init(operation: .delete, txids: [301])
                ),
            ],
            state: testShapeState(offset: "5_0"),
            schema: [:],
            reachedUpToDate: false
        )
        let result = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            batch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: batch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: result.resolvedTransactionIDs
        )

        try await waitForCompletion

        #expect(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).isEmpty)
        let pendingAfter = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pendingAfter.status == .resolved)
    }

    @Test("Failed optimistic delete keeps row visible and retryable")
    func failedDeleteRemainsRecoverable() async throws {
        enum SampleError: Error { case sendFailed }

        let container = try makeTestContainer()
        let setupContext = ModelContext(container)
        setupContext.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Existing"))
        try setupContext.save()

        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onDelete: { _ in throw SampleError.sendFailed }
        )

        let transaction = try await collection.delete("todo-1")

        do {
            try await transaction.awaitCompletion()
            Issue.record("Expected delete completion wait to fail")
        } catch {
            #expect(Bool(true))
        }

        let context = ModelContext(container)
        let todo = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(todo.collectionSyncState == .syncError)
        #expect(todo.collectionPendingMutationCount == 1)

        let pending = try #require(context.fetch(FetchDescriptor<ElectricPendingMutation>()).first)
        #expect(pending.status == .failed)
        #expect(pending.operation == .delete)
        #expect(pending.originalRow?["title"] == .string("Existing"))
    }

    @Test("Insert and delete in one transaction cancel out without persistence")
    func createAndDeleteInSingleTransactionCancelsOut() async throws {
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) },
            onDelete: { _ in ElectricMutationSubmission(awaitedTXIDs: [202]) }
        )

        let transaction = try await collection.transaction { builder in
            try builder.insert {
                TestTodo(id: "todo-1", projectID: "project-a", title: "Draft")
            }
            try builder.delete("todo-1")
        }

        #expect(await transaction.status == .completed)

        let context = ModelContext(container)
        #expect(try context.fetch(FetchDescriptor<TestTodo>()).isEmpty)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingTransaction>()).isEmpty)
        #expect(try context.fetch(FetchDescriptor<ElectricPendingMutation>()).isEmpty)
    }

    @Test("Write tracer records transaction lifecycle")
    func writeTracerRecordsTransactionLifecycle() async throws {
        let recorder = TestWriteTraceRecorder()
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container,
            writeTracer: recorder.tracer()
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) }
        )

        let transaction = try await collection.insert {
            TestTodo(id: "todo-1", projectID: "project-a", title: "Inserted")
        }

        let context = ModelContext(container)
        let batch = ShapeBatch(
            messages: [
                ElectricMessage(
                    key: "\"public\".\"todos\"/todo-1",
                    value: testTodoRow(title: "Inserted"),
                    headers: .init(operation: .insert, txids: [101])
                ),
            ],
            state: testShapeState(offset: "4_0"),
            schema: [:],
            reachedUpToDate: false
        )
        let result = try ElectricCollectionSynchronizer(identifier: testTodoIdentifier).apply(
            batch,
            shapeID: collection.shapeID,
            in: context
        )
        await database.shapeStoreDidApply(
            batch: batch,
            shapeID: collection.shapeID,
            resolvedTransactionIDs: result.resolvedTransactionIDs
        )

        try await transaction.awaitCompletion()
        let transactionID = await transaction.id

        let allEvents = recorder.events
        let events = allEvents.filter { $0.transactionID == transactionID }
        let kinds = events.map(\.kind)
        #expect(kinds == [
            .transactionStarted,
            .optimisticMutationRecorded,
            .transactionPersisted,
            .dispatchEnqueued,
            .dispatchStarted,
            .handlerInvoked,
            .handlerReturned,
            .awaitingSync,
            .transactionCompleted,
        ])

        let batchEvent = try #require(allEvents.first(where: { $0.kind == .shapeBatchApplied }))
        #expect(batchEvent.observedTXIDs == [101])
        #expect(batchEvent.offset == "4_0")

        let awaited = try #require(events.first(where: { $0.kind == .awaitingSync }))
        #expect(awaited.awaitedTXIDs == [101])

        let completed = try #require(events.last)
        #expect(completed.kind == .transactionCompleted)
        #expect(completed.observedTXIDs == [101])
    }

    @Test("Write tracer records merged same-key mutations")
    func writeTracerRecordsMergedMutations() async throws {
        let recorder = TestWriteTraceRecorder()
        let container = try makeTestContainer()
        let database = ElectricCollectionStore(
            shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
            modelContainer: container,
            writeTracer: recorder.tracer()
        )

        let collection = try await database.collection(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos",
            onInsert: { _ in ElectricMutationSubmission(awaitedTXIDs: [101]) }
        )

        let transaction = try await collection.transaction { builder in
            try builder.insert {
                TestTodo(id: "todo-1", projectID: "project-a", title: "Draft")
            }
            try builder.update("todo-1") { todo in
                todo.title = "Published"
            }
        }
        let transactionID = await transaction.id

        let mergeEvent = try #require(
            recorder.events.first(where: { event in
                event.transactionID == transactionID && event.kind == .mutationMerged
            })
        )
        #expect(mergeEvent.message == "coalesced create+update -> create")
    }
}
