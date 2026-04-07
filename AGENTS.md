# AGENTS

## Project Purpose

`electric-swift-data` is a Swift and SwiftData client for ElectricSQL.

The project is intentionally SwiftData-first:

- SwiftData is the only reactive row/query layer for app UI.
- `ElectricSwiftDataCollection` is a write-coordination and collection-lifecycle layer, not a second database.
- Do not add a parallel in-memory collection row store.

## Layered Architecture

- `ElectricSwift`
  Low-level ElectricSQL transport/runtime, `ElectricShapeSession`, SSE/long-poll coordination, decoding, shape state.
- `ElectricSwiftData`
  Read-side sync into SwiftData; owns shape subscriptions and row materialization.
- `ElectricSwiftDataCollection`
  Collection-scoped optimistic writes, durable transactions, retry/replay, lifecycle, and txid reconciliation.

## Core Architectural Decisions

- Keep SwiftData as the single source of truth for UI reads.
- Do not port TanStack DB’s query/index/live-query engine.
- Do port TanStack DB’s transaction semantics and mutation-handling discipline.
- Each collection is its own state engine for lifecycle and mutation coordination, but not for row storage.
- `ElectricCollectionStore` is a thin shared-infrastructure entry point only.

## Responsibilities By Type

- `ElectricCollectionStore`
  Shared dependencies and collection factory. Owns the shared `ElectricShapeStore` and routes shape updates to per-collection coordinators.
- `ElectricCollection<Model, ID>`
  Public API for one collection. Thin facade over a coordinator and subscription.
- `ElectricCollectionCoordinator<Model>`
  Internal runtime for one collection. Owns lifecycle, replay, dispatch, txid tracking, and optimistic transaction persistence.
- `ElectricShapeStore`
  Shared read-sync engine.
- `ElectricSwiftDataRowApplier`
  Generic row applier for Electric shape batches into SwiftData.
- `ElectricCollectionSynchronizer<Model>`
  Collection-aware synchronizer that reconciles incoming rows with pending local mutations and txids.
- `ElectricPendingTransaction` / `ElectricPendingMutation`
  Durable outbox state.
- `ElectricMutationDispatcher`
  Groups pending mutations by operation and executes outbound mutation handlers.
- `ElectricMutationReconciler`
  Resolves observed txids, finalizes transactions, and refreshes row sync state.

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

## SwiftData Guidance

- UI should query SwiftData directly via `@Query` / `FetchDescriptor`.
- Collection code may update SwiftData models optimistically.
- The model's primary `id` must be a stable, globally unique, immutable sync identifier.
- Collections declare that identifier via `ElectricModelIdentifier`; do not require a model-level `electricID` property or a second stored sync key.
- Keep additional row sync metadata minimal:
  - `electricSyncState`
  - `electricPendingMutationCount`
  - `electricLastLocalMutationAt`
  - `electricLastServerVersion`
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
5. Maintain clean separation between `ElectricSwiftData` read-side concerns and collection-layer write-side concerns.
6. Add tests for transaction semantics, replay, reconciliation, and lifecycle transitions before broadening APIs.

## Implementation Conventions

- Prefer small focused types over one large runtime object.
- Put collection-layer persistence models under `ElectricSwiftDataCollection` when dependency boundaries allow it.
- Keep `ElectricSwiftData` generic; collection-aware reconciliation belongs in `ElectricSwiftDataCollection`.
- Keep timeout-free txid acknowledgement as the authoritative completion mechanism unless the architecture explicitly changes.
- When adding collection features, verify they do not introduce a second query/read model for the UI.

## Current Constraint

`ElectricShapeStore` now supports injected batch-applier closures and session-backed checkpoint hydration. Use the default `ElectricSwiftDataRowApplier` for generic SwiftData materialization, and inject `ElectricCollectionSynchronizer` when a collection needs mutation-aware reconciliation.
