# Roadmap

This roadmap tracks the path to `v0.1` for `SwiftDataCollection`.

The package is intentionally split into two deliverables:

1. `SwiftDataCollection`
   The SwiftData-first collection core: optimistic local mutations, transaction-first durable outbox state, replay, retry, lifecycle, and backend-neutral row materialization.
2. `ElectricSwiftDataCollection`
   The first adapter: Electric shape subscriptions, Electric row conversion, SwiftData materialization, and txid-backed authoritative completion. It depends on `ElectricSwift` v0.1.0 from GitHub.

## Release Position

The collection path is restart-grade: transaction durability is transaction-first on disk, optimistic deletes are recoverable, retry/replay is autonomous, one managed shape or collection per model type is enforced, and the release-grade evidence bar is in place.

The Electric adapter now contains the old read-side SwiftData sync pieces, so there is no separate `ElectricSwiftData` module in this package.

## v0.1 Scope

### Release Goal

Ship a trustworthy SwiftData-first collection runtime with an Electric adapter implementation.

### Non-Goals

- porting TanStack DB live-query or index infrastructure
- adding a second in-memory row store beside SwiftData
- exposing Electric protocol types from `SwiftDataCollection`
- broad TypeScript API parity where a Swift-native API is clearer

## P0 Before v0.1

The original collection release blockers are now addressed:

- restart-grade recovery coverage exists for durable outbox replay, failed delete visibility, retry after reopen, FIFO ordering after restart, and atomic rollback persistence guarantees
- high-fidelity protocol-contract tests prove authoritative txid-backed completion against the Electric message surface the Swift runtime consumes
- `SwiftDataCollection` is backend-neutral and no longer imports `ElectricSwift`
- `ElectricSwiftDataCollection` is the Electric adapter target under `Sources/Adapters/ElectricSwiftDataCollection`
- `Package.resolved` pins `electric-swift` to `0.1.0`

The remaining `v0.1` work is release packaging and polish, not core collection correctness.

## P1 After v0.1

### 1. Adapter Utility Ergonomics

- Thread `ElectricCollectionSyncUtilities` through richer handler contexts when the public API needs it.
- Keep utilities in `ElectricSwiftDataCollection`, not `SwiftDataCollection`.

Why:

- Txid and raw Electric message matching are adapter concepts, while core completion should stay sync-agnostic.

### 2. Dynamic Headers and Parameters

- Support deferred or async resolution of request headers and extra parameters.

Why:

- Swift apps often need rotating auth tokens or user-scoped request parameters.

### 3. Diagnostics Surface

- Expose first-class diagnostics values such as last sync age, current replay state, and fallback mode.
- Consider a programmatic debug event stream in addition to log sinks.

Why:

- These systems fail in operational edge cases.
- Good diagnostics materially reduce support and debugging cost.

### 4. Broader Postgres Type Coverage

- Expand schema-driven coercion beyond the currently implemented common scalars, arrays, and JSON shapes.

Why:

- The current coverage is pragmatic, but not complete.

### 5. Outbox Cleanup and Compaction

- Prune or compact resolved transactions and mutations.
- Define retention rules for debugging versus storage growth.

Why:

- The collection layer should not accumulate durable bookkeeping without bounds.

### 6. Snapshot and Subset APIs

- Add explicit snapshot and subset support where it fits the SwiftData-first model.
- Define how partial-sync or subset data interacts with SwiftData reads.

Why:

- The TypeScript and TanStack references support richer partial-sync flows.
- This is useful, but not required for the first trustworthy release.

### 7. Internal State-Machine Refinement

- Continue splitting collection runtime behavior into focused components instead of growing the coordinator.

Why:

- The current direction is good, but the collection path still carries many responsibilities in one runtime.

## Design Guidance

The TypeScript and TanStack references remain behavioral references, not implementation templates.

### What To Port

- transaction-first durability discipline
- replay on restart
- autonomous retry scheduling
- evidence-based completion using observed sync state
- collection-scoped metadata and reset behavior
- adapter-owned protocol translation and confirmation semantics

### What Not To Port Literally

- TanStack DB’s live-query engine
- TanStack DB’s index/query runtime
- a second optimistic row store layered over SwiftData
- browser-specific storage and coordination assumptions
- TypeScript-style utility bags when typed Swift facades are clearer

## Changelog

### Implemented

- Introduced `SwiftDataCollection` as the backend-neutral core product.
- Moved the Electric adapter implementation to `Sources/Adapters/ElectricSwiftDataCollection`.
- Merged the old read-side SwiftData sync code into `ElectricSwiftDataCollection`.
- Added the GitHub `electric-swift` v0.1.0 package dependency.
- Added neutral core row and model types:
  - `CollectionValue`
  - `CollectionRow`
  - `CollectionRowDecoder`
  - `CollectionModelIdentifier`
- Replaced Electric-prefixed core transaction/outbox/tracing names with neutral core names.
- Kept Electric-specific handler confirmation in the adapter via `ElectricMutationSubmission`.
- Kept durable core completion metadata opaque as observation tokens instead of public txids.
- Added Electric row conversion at the adapter boundary.
- Added Electric adapter utilities for `awaitTxID` and `awaitMatch`.
- Added neutral row patching with protected pending fields.
- Preserved transaction-first mutation merge behavior for same-key mutations within a transaction.
- Deferred physical delete until authoritative sync completion so failed deletes remain visible and recoverable.
- Added autonomous replay and retry scheduling with startup, refresh, shape-apply, and foreground wake-up triggers.
- Rebuild pending local row-visible state from the durable outbox during collection bootstrap.
- Hard-enforced the one managed shape or collection per model type rule within `SwiftDataCollectionStore`.
- Preserved tests for:
  - sparse SwiftData updates
  - patch-aware reconciliation
  - transaction lifecycle tracing
  - txid-driven Electric completion
  - atomic commit rollback behavior
  - deferred optimistic delete recovery
  - autonomous retry after failure
  - managed shape/collection conflict enforcement
  - file-backed restart and recovery behavior

### Current Known Constraints

- SwiftData sync currently enforces one managed shape or collection per model type within a `SwiftDataCollectionStore`.
- The automated confidence bar is restart-grade persistence coverage plus high-fidelity protocol-contract tests, not a full live backend E2E lane.
- Dynamic headers and parameters are not yet supported.
- Postgres coercion coverage is still incomplete.
