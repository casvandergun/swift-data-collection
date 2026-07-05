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

## Offline and Retry Behavior

`SwiftDataCollectionStore` is network-aware by default on Apple platforms through `NWPathMonitor`. Local mutations are still accepted while offline: they are applied optimistically to SwiftData and persisted to the durable outbox, but outbound mutation handlers are not invoked until connectivity returns.

When the monitor reports online again, failed transactions are made eligible immediately and the outbox drain resumes. Connectivity is only a scheduling signal; backend or validation failures from mutation handlers still determine whether a transaction retries or stops.

Handlers are at-least-once: a transaction can be replayed after process restart, reconnect, or retry. Use stable transaction IDs or idempotency keys with your backend when a mutation endpoint is not naturally idempotent.

Throw `CollectionNonRetriableError` from a handler for permanent application failures such as validation, authorization, or unrecoverable conflict errors. The transaction and mutations are marked conflicted instead of being retried, and the affected row remains in `syncError`.

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
