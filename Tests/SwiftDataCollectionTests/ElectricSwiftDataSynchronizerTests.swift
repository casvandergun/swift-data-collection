@testable import ElectricSwift
@testable import ElectricSwiftDataCollection
import SwiftData
import Testing

@Suite("Electric SwiftData Batch Application")
struct ElectricSwiftDataBatchApplicationTests {
    @Test("Applies insert update delete and metadata")
    func appliesChangeBatches() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let synchronizer = ElectricSwiftDataRowApplier(identifier: testTodoIdentifier)

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: testTodoRow(title: "Initial"),
                        headers: .init(operation: .insert)
                    ),
                    .upToDate(),
                ],
                state: testShapeState(offset: "1_0"),
                schema: [:],
                reachedUpToDate: true
            ),
            shapeID: "todos",
            in: context
        )

        let inserted = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(inserted.title == "Initial")

        let metadataAfterInsert = try #require(
            context.fetch(
                FetchDescriptor<ElectricShapeMetadata>(
                    predicate: #Predicate<ElectricShapeMetadata> { $0.shapeID == "todos" }
                )
            ).first
        )
        #expect(metadataAfterInsert.offset == "1_0")
        #expect(metadataAfterInsert.isUpToDate == true)

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: [
                            "title": .string("Updated"),
                        ],
                        headers: .init(operation: .update)
                    ),
                ],
                state: testShapeState(offset: "2_0"),
                schema: [:],
                reachedUpToDate: false
            ),
            shapeID: "todos",
            in: context
        )

        let updated = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(updated.title == "Updated")
        #expect(updated.projectID == "project-a")

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        headers: .init(operation: .delete)
                    ),
                ],
                state: testShapeState(offset: "3_0"),
                schema: [:],
                reachedUpToDate: false
            ),
            shapeID: "todos",
            in: context
        )

        let deleted = try context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1"))
        #expect(deleted.isEmpty)
    }

    @Test("Must refetch clears previously materialized rows")
    func mustRefetchClearsRows() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let synchronizer = ElectricSwiftDataRowApplier(identifier: testTodoIdentifier)

        context.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Keep"))
        try context.save()

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [.mustRefetch()],
                state: testShapeState(handle: "shape-2", offset: "-1", isUpToDate: false),
                schema: [:],
                reachedUpToDate: false
            ),
            shapeID: "todos",
            in: context
        )

        let rows = try context.fetch(FetchDescriptor<TestTodo>())
        #expect(rows.isEmpty)
    }

    @Test("Pending local fields are protected during sync")
    func protectsPendingLocalFields() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let synchronizer = ElectricCollectionSynchronizer(identifier: testTodoIdentifier)

        let todo = TestTodo(
            collectionSyncStateRawValue: ElectricSyncState.pendingUpdate.rawValue,
            collectionPendingMutationCount: 1,
            id: "todo-1",
            projectID: "project-a",
            title: "Local Title"
        )
        context.insert(todo)
        context.insert(
            try makePendingMutation(
                targetKey: "todo-1",
                payload: testTodoRow(id: "todo-1", projectID: "project-a", title: "Local Title"),
                changedFields: ["title"],
                originalRow: testTodoRow(id: "todo-1", projectID: "project-a", title: "Server Title")
            )
        )
        try context.save()

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: [
                            "projectID": .string("project-b"),
                        ],
                        headers: .init(operation: .update)
                    ),
                ],
                state: testShapeState(offset: "5_0"),
                schema: [:],
                reachedUpToDate: false
            ),
            shapeID: "todos",
            in: context
        )

        let updated = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(updated.id == "todo-1")
        #expect(updated.title == "Local Title")
        #expect(updated.projectID == "project-b")
        #expect(updated.collectionSyncState == .pendingUpdate)
    }

    @Test("Sparse update preserves required untouched fields in generic SwiftData path")
    func sparseUpdatePreservesRequiredFields() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let synchronizer = ElectricSwiftDataRowApplier(identifier: testTodoIdentifier)

        context.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Initial"))
        try context.save()

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: [
                            "title": .string("Updated"),
                        ],
                        headers: .init(operation: .update)
                    ),
                ],
                state: testShapeState(offset: "4_0"),
                schema: [:],
                reachedUpToDate: false
            ),
            shapeID: "todos",
            in: context
        )

        let updated = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(updated.id == "todo-1")
        #expect(updated.projectID == "project-a")
        #expect(updated.title == "Updated")
    }

    @Test("Sequential insert and sparse updates in one batch materialize a full final row")
    func sequentialInsertAndSparseUpdatesInOneBatch() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let synchronizer = ElectricSwiftDataRowApplier(identifier: testTodoIdentifier)

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: testTodoRow(id: "todo-1", projectID: "project-a", title: "Initial"),
                        headers: .init(operation: .insert)
                    ),
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: [
                            "title": .string("Renamed"),
                        ],
                        headers: .init(operation: .update)
                    ),
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: [
                            "projectID": .string("project-b"),
                        ],
                        headers: .init(operation: .update)
                    ),
                    .upToDate(),
                ],
                state: testShapeState(offset: "5_0"),
                schema: [:],
                reachedUpToDate: true
            ),
            shapeID: "todos",
            in: context
        )

        let updated = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(updated.id == "todo-1")
        #expect(updated.projectID == "project-b")
        #expect(updated.title == "Renamed")
    }

    @Test("Sparse update preserves required untouched fields in collection synchronizer")
    func collectionSynchronizerSparseUpdatePreservesRequiredFields() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let synchronizer = ElectricCollectionSynchronizer(identifier: testTodoIdentifier)

        context.insert(TestTodo(id: "todo-1", projectID: "project-a", title: "Initial"))
        try context.save()

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: [
                            "title": .string("Updated"),
                        ],
                        headers: .init(operation: .update, txids: [222])
                    ),
                ],
                state: testShapeState(offset: "6_0"),
                schema: [:],
                reachedUpToDate: false
            ),
            shapeID: "todos",
            in: context
        )

        let updated = try #require(context.fetch(testTodoIdentifier.fetchDescriptor(for: "todo-1")).first)
        #expect(updated.id == "todo-1")
        #expect(updated.projectID == "project-a")
        #expect(updated.title == "Updated")
    }

    @Test("Row applier emits SwiftData apply debug events")
    func rowApplierEmitsDebugEvents() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let recorder = TestDebugRecorder()
        let synchronizer = ElectricSwiftDataRowApplier(
            identifier: testTodoIdentifier,
            debugLogger: recorder.logger()
        )

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: testTodoRow(title: "Initial"),
                        headers: .init(operation: .insert, txids: [301])
                    ),
                    .upToDate(),
                ],
                state: testShapeState(offset: "10_0"),
                schema: [:],
                reachedUpToDate: true
            ),
            shapeID: "todos",
            in: context
        )

        let events = recorder.events
        let applyEvents = events.filter { $0.category == "ShapeApply" }
        let insertEvent = try #require(
            applyEvents.first(where: { $0.metadata["key"] == "todo-1" })
        )
        #expect(insertEvent.metadata["txids"] == "301")
        #expect(insertEvent.metadata["offset"] == "10_0")
        #expect(insertEvent.metadata["outcome"] == "inserted")

        let skipEvent = try #require(
            applyEvents.first(where: { $0.message == "skipped message during SwiftData apply" })
        )
        #expect(skipEvent.metadata["control"] == ElectricControl.upToDate.rawValue)
        #expect(skipEvent.metadata["reason"] == "no operation")

        let metadataEvent = try #require(
            applyEvents.first(where: { $0.message == "updated shape metadata" })
        )
        #expect(metadataEvent.metadata["boundary"] == ElectricShapeBoundaryKind.upToDate.rawValue)

        let saveEvent = try #require(
            applyEvents.first(where: { $0.message == "saved SwiftData batch" })
        )
        #expect(saveEvent.metadata["messages"] == "2")
        #expect(saveEvent.metadata["observedTXIDs"] == "301")
    }

    @Test("Collection synchronizer emits reconciliation debug events")
    func collectionSynchronizerEmitsDebugEvents() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let recorder = TestDebugRecorder()
        let synchronizer = ElectricCollectionSynchronizer(
            identifier: testTodoIdentifier,
            collectionID: "TestTodo:todos",
            debugLogger: recorder.logger()
        )

        let todo = TestTodo(
            collectionSyncStateRawValue: ElectricSyncState.pendingUpdate.rawValue,
            collectionPendingMutationCount: 1,
            id: "todo-1",
            projectID: "project-a",
            title: "Local Title"
        )
        context.insert(todo)
        context.insert(
            try makePendingMutation(
                targetKey: "todo-1",
                payload: testTodoRow(id: "todo-1", projectID: "project-a", title: "Local Title"),
                changedFields: ["title"],
                originalRow: testTodoRow(id: "todo-1", projectID: "project-a", title: "Server Title")
            )
        )
        try context.save()

        _ = try synchronizer.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/todo-1",
                        value: testTodoRow(id: "todo-1", projectID: "project-b", title: "Server Title"),
                        headers: .init(operation: .update, txids: [401])
                    ),
                ],
                state: testShapeState(offset: "11_0"),
                schema: [:],
                reachedUpToDate: false
            ),
            shapeID: "todos",
            in: context
        )

        let mergeEvent = try #require(
            recorder.events.first(where: {
                $0.category == "ShapeApply"
                    && $0.message == "merged server row into pending local model"
            })
        )
        #expect(mergeEvent.metadata["shapeID"] == "todos")
        #expect(mergeEvent.metadata["collectionID"] == "TestTodo:todos")
        #expect(mergeEvent.metadata["outcome"] == "mergedPending")
        #expect(mergeEvent.metadata["txids"] == "401")
        #expect(mergeEvent.metadata["protectedFields"] == "title")
    }
}

@Suite("Electric Shape Store")
struct ElectricShapeStoreTests {
    @Test("Hydrates persisted checkpoint before starting session")
    func hydratesPersistedCheckpoint() async throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(
            ElectricShapeMetadata(
                shapeID: "todos",
                handle: "persisted-handle",
                offset: "42_0",
                cursor: "persisted-cursor",
                lastSyncedAt: Date()
            )
        )
        try context.save()

        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape-store")!
        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "persisted-handle",
                    "electric-offset": "42_0",
                    "electric-cursor": "persisted-cursor",
                    "electric-schema": "{}",
                ]
            )
        )

        let store = ElectricShapeStore(
            shapeURL: url,
            modelContainer: container,
            session: session
        )
        let subscription = await store.shape(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        await subscription.start()
        let checkpoint = try #require(await subscription.checkpoint)
        let phase = try #require(await subscription.phase)
        await subscription.stop()

        #expect(checkpoint.offset == "42_0")
        #expect(checkpoint.handle == "persisted-handle")
        #expect(checkpoint.cursor == "persisted-cursor")
        #expect(phase == .initial)
    }

    @Test("Subscription pause and resume preserve the managed runtime state")
    func subscriptionPauseAndResumePreserveRuntimeState() async throws {
        let container = try makeTestContainer()
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape-store")!
        let headers = [
            "electric-handle": "h-live",
            "electric-offset": "2_0",
            "electric-cursor": "cursor-live",
            "electric-schema": "{}",
        ]

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers),
            delayMilliseconds: 5_000
        )
        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers)
        )
        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers),
            delayMilliseconds: 5_000
        )

        let store = ElectricShapeStore(
            shapeURL: url,
            modelContainer: container,
            sessionFactory: { options, initialState, debugLogger in
                ShapeStream(
                    options: options,
                    configuration: .init(
                        subscribe: true,
                        initialState: initialState,
                        preferSSE: false
                    ),
                    transport: transport,
                    debugLogger: debugLogger
                )
            }
        )
        let subscription = await store.shape(
            TestTodo.self,
            identifier: testTodoIdentifier,
            table: "todos"
        )

        await subscription.start()
        try await Task.sleep(nanoseconds: 50_000_000)

        await subscription.pause()
        #expect(await subscription.status == ElectricSubscriptionStatus.paused)
        #expect(await subscription.phase == ElectricShapePhase.paused)

        await subscription.resume()

        let deadline = Date().addingTimeInterval(1)
        while await subscription.status != ElectricSubscriptionStatus.upToDate && Date() < deadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }

        let checkpoint = try #require(await subscription.checkpoint)
        #expect(await subscription.status == ElectricSubscriptionStatus.upToDate)
        #expect(checkpoint.handle == "h-live")
        #expect(checkpoint.offset == "2_0")

        let requests = await transport.requests()
        #expect(requests.count >= 2)
        let secondRequest = try #require(requests.dropFirst().first)
        let secondURL = try #require(secondRequest.url)
        let components = try #require(URLComponents(url: secondURL, resolvingAgainstBaseURL: false))
        #expect(components.queryItems?.contains(where: { $0.name == "live" }) == false)

        await subscription.stop()
    }
}
