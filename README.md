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

## Model Requirements

Collection-backed SwiftData models conform to `SwiftDataCollectionModel` and carry only minimal sync metadata:

```swift
import SwiftDataCollection
import SwiftData

@Model
final class Todo: SwiftDataCollectionModel {
    var collectionSyncState: CollectionSyncState
    var collectionPendingMutationCount: Int
    var id: String
    var projectID: String
    var title: String

    init(
        collectionSyncState: CollectionSyncState = .synced,
        collectionPendingMutationCount: Int = 0,
        id: String,
        projectID: String,
        title: String
    ) {
        self.collectionSyncState = collectionSyncState
        self.collectionPendingMutationCount = collectionPendingMutationCount
        self.id = id
        self.projectID = projectID
        self.title = title
    }

    convenience init(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws {
        let value = try decoder.decode(TodoValue.self, from: collectionRow)
        self.init(id: value.id, projectID: value.projectID, title: value.title)
    }

    func apply(collectionRow: CollectionRow, decoder: CollectionRowDecoder) throws {
        let value = try decoder.decode(TodoValue.self, from: collectionRow)
        id = value.id
        projectID = value.projectID
        title = value.title
    }

    func collectionRow() throws -> CollectionRow {
        [
            "id": .string(id),
            "projectID": .string(projectID),
            "title": .string(title),
        ]
    }
}

private struct TodoValue: Decodable {
    let id: String
    let projectID: String
    let title: String
}
```

The model’s primary sync identifier must be stable, globally unique, and immutable. Collections declare that identifier with `CollectionModelIdentifier`; they do not require a second stored sync key.

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
