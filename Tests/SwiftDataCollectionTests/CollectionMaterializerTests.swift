@testable import SwiftDataCollection
import Foundation
import SwiftData
import Testing

@Suite("Collection authoritative materialization")
struct CollectionMaterializerTests {
    private let collectionID = "materializer-tests"
    private let modelName = String(reflecting: TestTodo.self)

    @Test("authoritative patches update the base without overwriting optimistic fields")
    func patchBelowOptimisticUpdate() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "1", projectID: "server-a", title: "Server"))
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)
        try insertOverlay(
            context: context,
            sequence: 1,
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "server-a", title: "Local"),
            changedFields: ["title"]
        )

        try materializer.apply(.patch(["projectID": .string("server-b")]), for: "1")
        try materializer.materialize(key: "1")

        let row = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(row.title == "Local")
        #expect(row.projectID == "server-b")
        #expect(row.collectionSyncState == .pendingUpdate)
        #expect(
            try materializer.baselineEvidence(for: "1") ==
            .observedRow(testTodoCollectionRow(id: "1", projectID: "server-b", title: "Server"))
        )
    }

    @Test("the first captured baseline survives later optimistic model changes")
    func baselineIsCapturedBeforeOptimisticChanges() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let model = TestTodo(id: "1", projectID: "server", title: "Server")
        context.insert(model)
        let materializer = makeMaterializer(context)

        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)
        model.title = "Local"
        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)

        #expect(
            try materializer.baselineEvidence(for: "1") ==
            .observedRow(testTodoCollectionRow(id: "1", projectID: "server", title: "Server"))
        )
    }

    @Test("immediate updates patch the captured base instead of stale unchanged fields")
    func immediateUpdateUsesChangedFieldsOnly() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let model = TestTodo(id: "1", projectID: "server", title: "Before")
        context.insert(model)
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)

        // Simulate a caller's stale full modified representation. The
        // immediate contract carries only the changed field to the base.
        model.projectID = "stale"
        try materializer.accept(
            .update(["title": .string("Accepted")]),
            for: "1"
        )

        #expect(
            try materializer.baselineEvidence(for: "1") ==
            .acceptedRow(testTodoCollectionRow(id: "1", projectID: "server", title: "Accepted"))
        )
        try materializer.materialize(key: "1")
        let result = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(result.projectID == "server")
        #expect(result.title == "Accepted")
    }

    @Test("replacement, patch, and absence retain distinct authoritative provenance")
    func authoritativeProvenanceTransitions() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let materializer = makeMaterializer(context)
        let replacement = testTodoCollectionRow(id: "1", projectID: "server", title: "One")

        try materializer.apply(.replacement(replacement), for: "1")
        #expect(try materializer.baselineEvidence(for: "1") == .observedRow(replacement))

        try materializer.apply(.patch(["title": .string("Two")]), for: "1")
        #expect(
            try materializer.baselineEvidence(for: "1") ==
            .observedRow(testTodoCollectionRow(id: "1", projectID: "server", title: "Two"))
        )

        try materializer.apply(.absence, for: "1")
        #expect(try materializer.baselineEvidence(for: "1") == .absent)
    }

    @Test("transaction sequence orders overlays despite clock rollback")
    func sequenceOrderingWinsOverDates() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "1", projectID: "p", title: "Base"))
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)
        let now = Date()
        try insertOverlay(
            context: context,
            sequence: 2,
            createdAt: now.addingTimeInterval(-100),
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "p", title: "Second"),
            changedFields: ["title"]
        )
        try insertOverlay(
            context: context,
            sequence: 1,
            createdAt: now,
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "p", title: "First"),
            changedFields: ["title"]
        )

        try materializer.materialize(key: "1")

        let row = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(row.title == "Second")
    }

    @Test("pending local delete preserves its recoverable row")
    func softDelete() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "1", projectID: "p", title: "Recoverable"))
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .delete)
        try insertOverlay(
            context: context,
            sequence: 1,
            operation: .delete,
            payload: [:],
            changedFields: []
        )

        try materializer.materialize(key: "1")

        let row = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(row.title == "Recoverable")
        #expect(row.collectionSyncState == .pendingDelete)
        #expect(row.collectionPendingMutationCount == 1)
    }

    @Test("server absence below an update does not resurrect a visible row")
    func absenceBelowUpdate() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "1", projectID: "p", title: "Old"))
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)
        try insertOverlay(
            context: context,
            sequence: 1,
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "p", title: "Local"),
            changedFields: ["title"]
        )

        try materializer.apply(.absence, for: "1")
        try materializer.materialize(key: "1")

        #expect(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).isEmpty)
        #expect(try materializer.baselineEvidence(for: "1") == .absent)
    }

    @Test("immediate completion records accepted rather than observed provenance")
    func acceptedProvenanceAndCleanup() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let materializer = makeMaterializer(context)
        let row = testTodoCollectionRow(id: "1", projectID: "p", title: "Accepted")
        try materializer.captureBaselineIfNeeded(for: "1", operation: .create)

        try materializer.accept(.create(row), for: "1")
        #expect(try materializer.baselineEvidence(for: "1") == .acceptedRow(row))

        try materializer.materialize(key: "1")
        let model = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(model.title == "Accepted")
        #expect(try materializer.baselineEvidence(for: "1") == nil)
    }

    @Test("discarded values are removed from unsent successor payloads")
    func rebuildsUnsubmittedSuccessor() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "1", projectID: "server", title: "Base"))
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)

        let refused = try insertOverlay(
            context: context,
            sequence: 1,
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "refused", title: "Base"),
            changedFields: ["projectID"],
            status: .conflicted
        )
        let successor = try insertOverlay(
            context: context,
            sequence: 2,
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "refused", title: "Successor"),
            changedFields: ["title"]
        )
        refused.mutation.status = .discarded
        refused.transaction.status = .discarded

        try materializer.rebuildNeverSubmittedSuccessorPayloads(for: ["1"])
        try materializer.materialize(key: "1")

        #expect(successor.mutation.payload["projectID"] == .string("server"))
        #expect(successor.mutation.payload["title"] == .string("Successor"))
        #expect(successor.mutation.changedFields == ["title"])
        let row = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(row.projectID == "server")
        #expect(row.title == "Successor")
    }

    @Test("unknown evidence survives patches and blocks destructive repair")
    func unknownEvidence() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "1", projectID: "p", title: "Optimistic"))
        let materializer = makeMaterializer(context)
        try materializer.ensureUnknownBaseline(for: "1")
        try materializer.apply(.patch(["title": .string("Partial")]), for: "1")

        #expect(try materializer.baselineEvidence(for: "1") == .unknown)
        #expect(throws: CollectionMaterializationError.unknownBaseline(key: "1")) {
            try materializer.rebuildNeverSubmittedSuccessorPayloads(for: ["1"])
        }
        let row = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(row.title == "Optimistic")
    }

    @Test("corrupt update payloads fail even when the authoritative base is absent")
    func corruptPayloadOnAbsentBaseFailsStrictly() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .create)
        let overlay = try insertOverlay(
            context: context,
            sequence: 1,
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "p", title: "Local"),
            changedFields: ["title"]
        )
        overlay.mutation.payloadData = Data("not-json".utf8)

        #expect(throws: CollectionMaterializationError.invalidPersistedRow(key: "1")) {
            try materializer.materialize(key: "1")
        }
    }

    @Test("submitted mutation payloads remain frozen during successor rebuild")
    func submittedPayloadIsNotRewritten() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "1", projectID: "server", title: "Base"))
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)
        let submitted = try insertOverlay(
            context: context,
            sequence: 1,
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "server", title: "Submitted"),
            changedFields: ["title"]
        )
        submitted.transaction.dispatchGroupID = submitted.transaction.id
        submitted.transaction.submittedMutationsData = try JSONEncoder().encode([
            CollectionMutation(
                operation: .update,
                key: "1",
                original: testTodoCollectionRow(id: "1", projectID: "server", title: "Base"),
                modified: testTodoCollectionRow(id: "1", projectID: "server", title: "Submitted"),
                changes: ["title": .string("Submitted")]
            )
        ])
        submitted.transaction.attemptCount = 0
        submitted.mutation.attemptCount = 1
        let payloadBefore = submitted.mutation.payloadData

        try materializer.rebuildNeverSubmittedSuccessorPayloads(for: ["1"])

        #expect(submitted.mutation.payloadData == payloadBefore)
    }

    @Test("partial evidence for an absent clean key does not retain a base")
    func incompletePatchForCleanAbsence() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        let materializer = makeMaterializer(context)

        try materializer.apply(.patch(["title": .string("Partial")]), for: "missing")

        #expect(try materializer.baselineEvidence(for: "missing") == nil)
        #expect(try materializer.baselineKeys().isEmpty)
    }

    @Test("an unused base never consumes a staged local row")
    func stagedRowSurvivesBaseCleanup() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(collectionSyncState: .stagedCreate, id: "1", projectID: "p", title: "Draft"))
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .create)

        try materializer.materialize(key: "1")

        let row = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(row.title == "Draft")
        #expect(row.collectionSyncState == .stagedCreate)
        #expect(try materializer.baselineEvidence(for: "1") == nil)
    }

    @Test("corrupt overlay rows fail instead of becoming absence")
    func corruptOverlayFailsStrictly() throws {
        let container = try makeTestContainer()
        let context = ModelContext(container)
        context.insert(TestTodo(id: "1", projectID: "p", title: "Base"))
        let materializer = makeMaterializer(context)
        try materializer.captureBaselineIfNeeded(for: "1", operation: .update)
        let overlay = try insertOverlay(
            context: context,
            sequence: 1,
            operation: .update,
            payload: testTodoCollectionRow(id: "1", projectID: "p", title: "Local"),
            changedFields: ["title"]
        )
        overlay.mutation.payloadData = Data("not-json".utf8)

        #expect(throws: CollectionMaterializationError.invalidPersistedRow(key: "1")) {
            try materializer.materialize(key: "1")
        }
        let row = try #require(try context.fetch(testTodoIdentifier.fetchDescriptor(for: "1")).first)
        #expect(row.title == "Base")
    }

    private func makeMaterializer(_ context: ModelContext) -> CollectionMaterializer<TestTodo, String> {
        CollectionMaterializer(
            context: context,
            collectionID: collectionID,
            modelName: modelName,
            identifier: testTodoIdentifier,
            rowDecoder: CollectionRowDecoder()
        )
    }

    @discardableResult
    private func insertOverlay(
        context: ModelContext,
        sequence: Int,
        createdAt: Date = Date(),
        operation: CollectionMutationOperation,
        payload: CollectionRow,
        changedFields: Set<String>,
        status: PendingMutationStatus = .pending
    ) throws -> (transaction: PendingCollectionTransaction, mutation: PendingCollectionMutation) {
        let transactionID = UUID()
        let transaction = PendingCollectionTransaction(
            id: transactionID,
            collectionID: collectionID,
            shapeID: "todos",
            modelName: modelName,
            sequenceNumber: sequence,
            status: status == .conflicted ? .conflicted : .pending,
            createdAt: createdAt,
            updatedAt: createdAt
        )
        let mutation = PendingCollectionMutation(
            transactionID: transactionID,
            modelName: modelName,
            shapeID: "todos",
            targetKey: "1",
            operation: operation,
            payloadData: try JSONEncoder().encode(payload),
            changedFieldsData: try JSONEncoder().encode(changedFields),
            status: status,
            createdAt: createdAt
        )
        context.insert(transaction)
        context.insert(mutation)
        return (transaction, mutation)
    }
}
