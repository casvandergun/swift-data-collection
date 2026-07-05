# AGENTS

## Project Purpose

`SwiftDataCollection` is a SwiftData-first collection runtime with an ElectricSQL adapter.

The project is intentionally SwiftData-first:

- SwiftData is the only reactive row/query layer for app UI.
- `SwiftDataCollection` is the backend-neutral write-coordination and collection-lifecycle core, not a second database.
- `ElectricSwiftDataCollection` is the Electric adapter layer.
- Do not add a parallel in-memory collection row store.

## Layered Architecture

- `ElectricSwift`
  Low-level ElectricSQL transport/runtime, `ElectricShapeSession`, SSE/long-poll coordination, decoding, shape state.
- `SwiftDataCollection`
  Backend-neutral collection core: optimistic local mutations, transaction-first durable outbox state, retry/replay, lifecycle, and adapter-driven row materialization.
- `ElectricSwiftDataCollection`
  Electric-backed adapter: shape subscriptions, Electric row conversion, SwiftData materialization, mutation-aware reconciliation, and txid-backed authoritative completion.

## Core Architectural Decisions

- Keep SwiftData as the single source of truth for UI reads.
- Do not port TanStack DB’s query/index/live-query engine.
- Do port TanStack DB’s transaction semantics and mutation-handling discipline.
- Each collection is its own state engine for lifecycle and mutation coordination, but not for row storage.
- `SwiftDataCollectionStore` is the shared-infrastructure entry point.
- Scheduling is currently per collection, not globally FIFO across all collections; `ROADMAP.md` is the source of truth for any planned cross-collection scheduler or dependency model.

## Responsibilities By Type

- `SwiftDataCollectionStore`
  Shared dependencies, collection factory, foreground wakeups, connectivity monitoring, and managed-model registration.
- `SwiftDataCollection<Model, ID>`
  Public API for one collection. Thin facade over a coordinator and subscription.
- `CollectionCoordinator<Model, ID>`
  Internal runtime for one collection. Owns lifecycle, replay, dispatch, txid tracking, and optimistic transaction persistence.
- `ElectricShapeStore`
  Electric adapter read-sync engine.
- `ElectricSwiftDataRowApplier`
  Generic row applier for Electric shape batches into SwiftData.
- `ElectricCollectionSynchronizer<Model>`
  Collection-aware synchronizer that reconciles incoming rows with pending local mutations and txids.
- `PendingCollectionTransaction` / `PendingCollectionMutation`
  Durable outbox state.
- `CollectionMutationDispatcher`
  Groups pending mutations by operation and executes outbound mutation handlers.
- `CollectionMutationReconciler`
  Resolves observed txids, finalizes transactions, and refreshes row sync state.
- `CollectionConnectivityMonitoring`
  Injectable connectivity source used to pause outbound dispatch while offline and resume eligible work on reconnect.

## Transaction Model

- Transactions are first-class and may contain multiple mutations.
- Direct `insert/update/delete` are convenience wrappers around implicit single-mutation transactions.
- Same-key mutations within a transaction must be coalesced using these rules:
  - `insert + update -> insert`
  - `insert + delete -> remove both`
  - `update + update -> merged update`
  - `update + delete -> delete`
- The outbox is transaction-first, not mutation-first.
- Completion is driven by observed Electric txids.
- Outbound handler execution is paused while offline; local optimistic writes remain durable and replay when connectivity returns.
- Retryable failures use bounded exponential backoff. `CollectionNonRetriableError` marks local transaction state conflicted instead of retrying.

## SwiftData Guidance

- UI should query SwiftData directly via `@Query` / `FetchDescriptor`.
- Collection code may update SwiftData models optimistically.
- The model's primary `id` must be a stable, globally unique, immutable sync identifier.
- Collections declare that identifier via `CollectionModelIdentifier`; do not require a model-level `electricID` property or a second stored sync key.
- Keep additional row sync metadata minimal:
  - `collectionSyncState`
  - `collectionPendingMutationCount`
- Keep collection metadata separate from row data.

## Non-Goals

- No adapter-style public API mirroring TanStack DB.
- No browser-specific offline mechanisms.
- No duplicate row cache layered over SwiftData.
- No live-query/query-compiler port in this effort.

## Evolution Priorities

1. Keep the package/module names aligned with `ElectricSwiftDataCollection`.
2. Preserve transaction-first outbox semantics.
3. Expand multi-mutation transaction handling and merge coverage.
4. Keep splitting collection behavior into focused components rather than regressing toward one coordinator/god object.
5. Maintain clean separation between backend-neutral `SwiftDataCollection` concerns and Electric adapter concerns.
6. Add tests for transaction semantics, replay, reconciliation, and lifecycle transitions before broadening APIs.
7. Keep `CHANGELOG.md` and `ROADMAP.md` current for every user-visible behavior, public API, release-plan, or architectural change.

## Implementation Conventions

- Prefer small focused types over one large runtime object.
- Put backend-neutral collection persistence models under `SwiftDataCollection`.
- Keep Electric protocol translation and collection-aware Electric reconciliation in `ElectricSwiftDataCollection`.
- Keep timeout-free txid acknowledgement as the authoritative completion mechanism unless the architecture explicitly changes.
- When adding collection features, verify they do not introduce a second query/read model for the UI.
- Treat `CHANGELOG.md` as the source of truth for released and unreleased user-visible changes.
- Treat `ROADMAP.md` as the source of truth for planned version targets, known constraints, and TanStack parity decisions.
- Any implementation that changes behavior, public API, release scope, or architecture must update the changelog and roadmap in the same work unless the user explicitly requests code-only exploration.
- Do not leave completed roadmap items listed as open work; move historical details to `CHANGELOG.md` and keep `ROADMAP.md` forward-looking.

## Current Constraint

`ElectricShapeStore` now supports injected batch-applier closures and session-backed checkpoint hydration. Use the default `ElectricSwiftDataRowApplier` for generic SwiftData materialization, and inject `ElectricCollectionSynchronizer` when a collection needs mutation-aware reconciliation.
