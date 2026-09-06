# SwiftDataCollection

`SwiftDataCollection` is a SwiftData-first collection runtime for optimistic writes, durable transactions, retry/replay, and collection lifecycle coordination.

The package is intentionally SwiftData-first:

- SwiftData is the only reactive row and query layer for app UI.
- `SwiftDataCollection` is the backend-neutral core entrypoint.
- `ElectricSwiftDataCollection` is the first adapter implementation.
- There is no second in-memory collection row store.

See [ROADMAP.md](/Users/casvandergun/Documents/projects/swift-data-collection/ROADMAP.md) for planned parity and runtime work.

## Package Layout

- `SwiftDataCollection`
  - `SwiftDataCollectionStore` as the shared infrastructure entrypoint
  - `SwiftDataCollection<Model, ID>` for typed collection APIs
  - `CollectionOptions` and `CollectionAdapter` for adapter-driven construction
  - `CollectionValue`, `CollectionRow`, `CollectionRowDecoder`, and `CollectionModelIdentifier`
  - transaction-first durable outbox models and optimistic write coordination

- `ElectricSwiftDataCollection`
  - Electric-backed `electricCollectionOptions(...)`
  - `ElectricCollectionOptions<Model, ID>`
  - `ElectricMutationSubmission.immediate`, `.txid(Int64)`, and `.txids(Set<Int64>)`
  - `ElectricCollectionSyncUtilities.awaitTxID(...)` and `awaitMatch(...)`
  - `ElectricShapeStore`, `ElectricShapeSubscription`, and `ElectricSwiftDataRowApplier`
  - `ElectricCollectionSynchronizer` for mutation-aware Electric reconciliation

- `FetchSwiftDataCollection`
  - fetch-backed `fetchCollectionOptions(...)`
  - complete-snapshot materialization into SwiftData
  - mutation handlers that complete after an automatic refresh
  - `.deleteSyncedRows` and `.keepLocalRows` missing-row policies

The adapter depends on [`ElectricSwift`](https://github.com/casvandergun/electric-swift) v0.1.0 for Electric protocol/runtime types.

## Architecture

The core/adapter split follows the TanStack collection-adapter responsibility model without porting TanStack DB’s in-memory query engine:

- `SwiftDataCollection` owns collection transactions, durable queue state, optimistic SwiftData writes, retry/replay, and lifecycle state.
- `ElectricSwiftDataCollection` owns Electric shape subscriptions, txid handling, Electric row translation, and shape-batch materialization into SwiftData.
- App UI continues to read SwiftData directly with `@Query` or `FetchDescriptor`.

Electric protocol rows are translated at the adapter boundary:

```text
ElectricRow / ElectricValue
  -> CollectionRow / CollectionValue
  -> SwiftDataCollectionModel
  -> SwiftData @Model
```

## Fetch-Backed Collection

Create the container with the runtime's metadata schema as well as your app models:

```swift
let schema = Schema(SwiftDataCollectionSchema.models(including: [Todo.self]))
let container = try ModelContainer(for: schema)
```

For Electric, include `ElectricShapeMetadata.self` in the application model list
too. The helper registers private write-coordination metadata; UI continues to
query `Todo` directly. Collection registration throws if required metadata is
missing from the supplied container.

```swift
import FetchSwiftDataCollection
import SwiftDataCollection
import SwiftData

let store = SwiftDataCollectionStore(modelContainer: container)

let todos = try await store.collection(
    Todo.self,
    options: fetchCollectionOptions(
        debugName: "Project todos",
        scopeID: "project:\(projectID)",
        identifier: todoIdentifier,
        fetch: { _ in
            try await api.todos(projectID: projectID).map(\.collectionRow)
        },
        onUpdate: { context in
            try await api.todos.update(context.mutations)
        }
    )
)

await todos.start()
```

The fetch adapter treats each successful fetch as the complete authoritative snapshot for the named `scopeID`. It does not manage dynamic subsets or overlapping query keys; bound the fetch closure itself, for example by project or account, when the server table is large. Mutation completion assumes the fetch endpoint provides adequate read-after-write behavior.

## Electric-Backed Collection

```swift
import ElectricSwiftDataCollection
import SwiftDataCollection
import SwiftData

let store = SwiftDataCollectionStore(modelContainer: container)

let todoIdentifier = CollectionModelIdentifier<Todo, String>.string(
    get: \.id,
    fetchDescriptor: { id in
        FetchDescriptor(predicate: #Predicate<Todo> { $0.id == id })
    }
)

let todos = try await store.collection(
    Todo.self,
    options: electricCollectionOptions(
        identifier: todoIdentifier,
        shapeURL: URL(string: "http://localhost:3000/v1/shape")!,
        table: "todos",
        where: "project_id = 'abc'",
        onUpdate: { context in
            var txids = Set<Int64>()
            for mutation in context.mutations {
                let response = try await api.todos.update(
                    id: mutation.key,
                    changes: mutation.changes
                )
                txids.insert(response.txid)
            }
            return .txids(txids)
        }
    )
)

await todos.start()

let tx = try await todos.transaction { transaction in
    try transaction.insert {
        Todo(id: "todo_01HT...", projectID: "abc", title: "Optimistic title")
    }
    try transaction.update("todo_01HT...") { todo in
        todo.title = "Updated optimistically"
    }
}

try await tx.awaitCompletion()
```

Electric handlers return Electric-specific confirmation:

- `.immediate` when the write is already authoritative.
- `.txid(txid)` when one backend txid must be observed.
- `.txids(txids)` when multiple txids must be observed.

Core handlers stay sync-agnostic and complete by returning or throwing. Txids do not appear in the core public API.

When a txid is not available, the Electric adapter also exposes `ElectricCollectionSyncUtilities.awaitMatch(...)` for waiting on a matching Electric message after its batch has been applied to SwiftData.

## Durable Staged Inserts

Use a staged insert when a row must exist durably in SwiftData before the application decides whether this client should publish it. A `.stagedCreate` row has no outbound mutation and has not yet been confirmed by the collection's synchronization adapter.

```text
                         adapter insert/update
                   ┌─────────────────────────────┐
                   │                             ▼
absent ──stage──▶ stagedCreate ──publish──▶ pendingCreate ──sync──▶ synced
                     │
                     └──discard─────────────────────────────▶ absent
```

```swift
let outcome = try await moments.stageInsert {
    Moment(id: proposedID, eventID: eventID, startedAt: startedAt)
}

try await moments.updateStaged(proposedID) { moment in
    moment.endedAt = endedAt
}

// If this client becomes responsible for the server write:
let transaction = try await moments.publishStagedInsert(
    proposedID,
    metadata: ["coordinationAttempt": .string(attemptID)]
)

// Or, after application-owned relationship rebinding:
try await moments.discardStagedInsert(proposedID)
```

Staging is idempotent. It returns `.inserted`, `.alreadyStaged`, or `.alreadySynced` and never replaces an existing row. Use `updateStaged` for intentional local changes. Publishing snapshots the staged row's latest values into the normal transaction-first durable outbox without inserting a second SwiftData model.

Only adapter observation, publication, or explicit discard leaves `.stagedCreate`. Timeouts never publish staged work automatically. Adapter deletes, missing Fetch snapshot rows, and Electric `must-refetch` cleanup preserve staged rows.

Managed adapter application and collection writes are serialized per collection. The relevant race outcomes are:

- `.alreadySynced` from `stageInsert` means adapter application won.
- `invalidStagedTransition(..., .synced)` from publication means adapter resolution won; continue with the synchronized row.
- The same error from discard means a normal collection delete is required if removal is still intended.
- A discarded row may be recreated by a later authoritative adapter insert.

Relationship rebinding and referential-integrity decisions remain application-owned. Phase one intentionally has no general authoritative-write API; WebSocket or API responses may decide which key the application stages, but only the synchronization adapter marks that row synchronized.

SwiftData does not support `CollectionSyncState` enum constants in predicates on the package's supported toolchain. To discover unresolved staged rows, fetch the appropriately scoped models and filter them:

```swift
let stagedMoments = try context.fetch(FetchDescriptor<Moment>())
    .filter { $0.collectionSyncState == .stagedCreate }
```

Concurrent use of `ElectricSwiftDataRowApplier` and a managed collection over the same model type is unsupported. The standalone applier preserves staged rows, but it does not participate in the managed collection's write gate.

## Offline and Retry Behavior

`SwiftDataCollectionStore` is network-aware by default on Apple platforms through `NWPathMonitor`. Local mutations are still accepted while offline: they are applied optimistically to SwiftData and persisted to the durable outbox, but outbound mutation handlers are not invoked until connectivity returns.

When the monitor reports online again, failed transactions are made eligible immediately and the outbox drain resumes. Connectivity is only a scheduling signal; backend or validation failures from mutation handlers still determine whether a transaction retries or stops.

Handlers are at-least-once: a transaction can be replayed after process restart, reconnect, or retry. Use stable transaction IDs or idempotency keys with your backend when a mutation endpoint is not naturally idempotent.

Throw `CollectionNonRetriableError` from a handler for permanent application failures such as validation, authorization, or unrecoverable conflict errors. The transaction and mutations are marked conflicted instead of being retried, and an existing affected row shows `.conflicted`. Retryable failures show `.error`.

## Inspecting And Discarding Conflicts

Conflicts are durable dispatch groups. A single transaction forms its own group;
compatible never-submitted transactions may be compacted into one request and
then share a conflict ID. A parked conflict blocks later dispatch on its keys;
unrelated keys continue.

```swift
let conflicts = try await todos.conflicts()
for conflict in conflicts where conflict.repairReadiness == .ready {
    // Call this after the application decides to abandon the local intent.
    try await todos.discard(conflict.id)
}

let updates = await todos.conflictUpdates
for try await snapshot in updates {
    // Display diagnostics; continue reading actual Todo rows with @Query.
    showConflicts(snapshot)
}
```

Each subscription begins with the current snapshot. Entries associate a key and
operation with local changes and baseline evidence. Unknown evidence makes
discard unavailable until authoritative data establishes a safe baseline.
Conflicts remain inspectable even when the server deleted the affected row.

Discard atomically removes a group's intent, rebuilds the affected SwiftData rows,
and repairs never-submitted successor representations. It does not undo writes
already accepted by the backend. Transaction waiters that failed remain failed.

Handlers may read `modified`, but its unchanged fields can be rebuilt before
first submission after an earlier intent is discarded. Submitted request bodies
and compacted group membership remain stable across automatic retries. Use
`context.transaction.id` as the backend idempotency key.

Immediate completion accepts the submitted representation without observing a
server-transformed row. For generated or normalized server values, use Electric
txid confirmation or the Fetch adapter's authoritative refresh. An update over
authoritative absence is not implicitly an insert.

## Upgrading To v0.2.0

This release requires an explicit source and schema migration. Replace
`.syncError` with `.error`, and `.awaitingSync` with `.awaiting`; the persisted
raw values change to `error` and `awaiting` respectively.
Register metadata through `SwiftDataCollectionSchema.models(including:)`.
Transparent migration of older dirty outboxes is not supported: older stores may
lack both authoritative bases and compacted request membership.

Applications own the transition. Drain or export pending/staged local work before
replacing a store, or implement an application-specific migration. Never discard
unsent user work as an incidental schema upgrade. The package does not delete or
reset an existing store automatically.

## Model Requirements

Collection-backed SwiftData models conform to `SwiftDataCollectionModel` and carry only minimal sync metadata:

```swift
import SwiftDataCollection
import SwiftData

@Model
final class Todo: SwiftDataCollectionModel {
    var collectionSyncState: CollectionSyncState
    var collectionPendingMutationCount: Int
    var id: UUID
    var projectID: UUID
    var title: String
    var completedAt: Date?

    init(
        collectionSyncState: CollectionSyncState = .synced,
        collectionPendingMutationCount: Int = 0,
        id: UUID,
        projectID: UUID,
        title: String,
        completedAt: Date? = nil
    ) {
        self.collectionSyncState = collectionSyncState
        self.collectionPendingMutationCount = collectionPendingMutationCount
        self.id = id
        self.projectID = projectID
        self.title = title
        self.completedAt = completedAt
    }

    convenience init(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws {
        let value = try decoder.decode(TodoValue.self, from: collectionRow)
        self.init(
            id: value.id,
            projectID: value.projectID,
            title: value.title,
            completedAt: value.completedAt
        )
    }

    func apply(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws {
        let value = try decoder.decode(TodoValue.self, from: collectionRow)
        id = value.id
        projectID = value.projectID
        title = value.title
        completedAt = value.completedAt
    }

    func collectionRow() throws -> CollectionRow {
        [
            "id": .uuid(id),
            "projectID": .uuid(projectID),
            "title": .string(title),
            "completedAt": .date(completedAt),
        ]
    }
}

private struct TodoValue: Decodable {
    let id: UUID
    let projectID: UUID
    let title: String
    let completedAt: Date?
}
```

The model’s primary sync identifier must be stable, globally unique, and immutable. Collections declare that identifier with `CollectionModelIdentifier`; they do not require a second stored sync key.

`ElectricRow` is the transport representation, `ElectricSchema` is database/transport metadata, and `CollectionRow` is the normalized app-side row. Use `CollectionSchema` when the collection should explicitly parse incoming Electric fields into SwiftData-native values:

```swift
electricCollectionOptions(
    identifier: todoIdentifier,
    shapeURL: shapeURL,
    table: "todos",
    collectionSchema: CollectionSchema([
        "id": .uuid,
        "projectID": .uuid,
        "completedAt": .date,
    ])
)
```

When Electric rows are translated into `CollectionRow`, `CollectionSchema` wins first, then Electric/Postgres schema inference is used as a fallback. Postgres `uuid` columns normalize into `.uuid(UUID)` values and temporal columns normalize into `.date(Date)` values.

## Release Confidence

The current confidence bar is restart-grade SwiftData persistence coverage plus adapter/protocol contract tests:

- replay, retry, pending-state rebuild, atomic rollback, and deferred delete recovery
- Electric sparse row materialization and `must-refetch` behavior
- txid-backed authoritative completion
- managed shape/collection conflict enforcement

Run the suite with:

```sh
swift test
```
