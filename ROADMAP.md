# Roadmap

This roadmap tracks the path to `v0.1` for `SwiftDataCollection`.

The package is intentionally split into two deliverables:

1. `SwiftDataCollection`
   The SwiftData-first collection core: optimistic local mutations, transaction-first durable outbox state, replay, retry, lifecycle, and backend-neutral row materialization.
2. `ElectricSwiftDataCollection`
   The first adapter: Electric shape subscriptions, Electric row conversion, SwiftData materialization, and txid-backed authoritative completion. It depends on `ElectricSwift` v0.1.0 from GitHub.

## Current Status

The collection path is restart-grade and offline-aware: transaction durability is transaction-first on disk, optimistic deletes are recoverable, retry/replay is autonomous, network loss pauses outbound dispatch, reconnect resumes eligible work, one managed shape or collection per model type is enforced, and the release-grade evidence bar is in place.

The Electric adapter now contains the old read-side SwiftData sync pieces, so there is no separate `ElectricSwiftData` module in this package.

The remaining `v0.1` work is release packaging and API polish, not core collection correctness.

## v0.1 Scope

### Release Goal

Ship a trustworthy SwiftData-first collection runtime with an Electric adapter implementation.

### Non-Goals

- porting TanStack DB live-query or index infrastructure
- adding a second in-memory row store beside SwiftData
- exposing Electric protocol types from `SwiftDataCollection`
- broad TypeScript API parity where a Swift-native API is clearer

## v0.1 Release Readiness

The original collection release blockers are addressed:

- restart-grade recovery coverage exists for durable outbox replay, failed delete visibility, retry after reopen, FIFO ordering after restart, and atomic rollback persistence guarantees
- high-fidelity protocol-contract tests prove authoritative txid-backed completion against the Electric message surface the Swift runtime consumes
- `SwiftDataCollection` is backend-neutral and no longer imports `ElectricSwift`
- `ElectricSwiftDataCollection` is the Electric adapter target under `Sources/Adapters/ElectricSwiftDataCollection`
- `Package.resolved` pins `electric-swift` to `0.1.0`
- retry delays are bounded and attempt counts saturate safely
- diagnostics levels and payload filtering are implemented
- offline connectivity gating is implemented with an injectable monitor and `NWPathMonitor` default

Before tagging `v0.1`, do a final packaging pass:

- review public API names and access levels for accidental `package`/`public` mismatches
- run the full Swift test suite with the Xcode toolchain
- update README examples if any public API names change
- tag and publish once the package manifest and dependency pin are final

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

- Diagnostics levels, payload filtering, app logger, handler diagnostics, and OSLog tracing are implemented.
- Still consider a programmatic debug event stream in addition to log sinks.
- Expose public outbox inspection values such as pending/running counts and queued transaction summaries.
- Expose first-class status snapshots such as last sync age, current replay state, current connectivity state, and pending outbox counts.

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
- Same-key successor update compaction during dispatch is implemented; this item is about durable historical cleanup, not outbound mutation merging.

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
- The latest direction is positive: retry policy, tracing, row application, synchronization, connectivity, and adapter runtime seams are separate enough to test directly.

Why:

- The collection coordinator still owns lifecycle, dispatch, retry, txid registration, pending-state refresh, and failure handling. Future features should move more policy into focused collaborators rather than adding more branches to the coordinator.

### 8. Cross-Collection Scheduling

- Consider a shared scheduler owned by `SwiftDataCollectionStore` for cross-collection dispatch order.
- Keep SwiftData as the only row/query store and keep per-collection coordinators responsible for adapter execution, txid registration, reconciliation, and row-state refresh.
- The scheduler would coordinate *when* a collection transaction may dispatch; it should not become a second row cache or adapter runtime.
- This is especially relevant for parent/child writes across collections, such as creating a parent row and then a child row that references it before the parent is authoritative.

Why:

- TanStack's offline transaction package uses a global executor because the browser package owns one durable outbox, one leader-election role, one online detector, named mutation functions, and optimistic state restoration across TanStack collections.
- SwiftDataCollection currently has per-collection coordinators because Electric completion, shape subscriptions, same-key safety, and row reconciliation are naturally collection/source scoped.
- Per-collection scheduling is simpler and avoids a coordinator god object, but it does not provide global FIFO across independent collections.
- A store-level scheduler is tempting as a middle ground: preserve collection-scoped state engines while adding explicit cross-collection ordering or dependency blocking.
- Do not jump directly to global serialization unless the package needs true multi-collection ordering; explicit dependencies such as `dependsOnTransactionIDs` or `dependsOnKeys` may solve parent/child constraints with less unnecessary blocking.

## Design Guidance

The TypeScript and TanStack references remain behavioral references, not implementation templates.

### What To Port

- transaction-first durability discipline
- replay on restart
- autonomous retry scheduling
- network-aware pause/resume with reconnect-triggered retry reset
- explicit non-retriable failure signaling
- evidence-based completion using observed sync state
- collection-scoped metadata and reset behavior
- adapter-owned protocol translation and confirmation semantics

### What Not To Port Literally

- TanStack DB’s live-query engine
- TanStack DB’s index/query runtime
- a second optimistic row store layered over SwiftData
- browser-specific storage and coordination assumptions
- TypeScript-style utility bags when typed Swift facades are clearer

### Current TanStack Offline-Transaction Diffs

The Swift implementation should match TanStack's behavior where it maps cleanly to SwiftData, but the following differences are intentional or still open:

- **Authoritative completion:** TanStack removes an outbox transaction when the mutation function succeeds. Electric-backed collections may remain `.awaitingSync` until observed txids or refresh completion prove that SwiftData has seen the authoritative write.
- **Permanent failures:** TanStack's `NonRetriableError` removes the transaction from the outbox and rejects waiters. SwiftDataCollection marks transaction and mutations `.conflicted` and leaves affected rows in `syncError` so the app can surface or repair local state.
- **Scheduling scope:** TanStack schedules through one global executor. SwiftDataCollection schedules per collection today; a future store-level scheduler may add cross-collection ordering without taking over row storage or adapter responsibilities.
- **Storage and leadership:** TanStack needs IndexedDB/localStorage fallback and Web Locks/BroadcastChannel leader election. SwiftDataCollection uses SwiftData durability and does not have browser tab leadership.
- **Outbox administration:** TanStack exposes `peekOutbox`, `removeFromOutbox`, `clearOutbox`, pending count, and running count. SwiftDataCollection has persisted outbox models and traces, but no polished public admin API yet.
- **Retry filtering:** TanStack has `beforeRetry` to filter loaded transactions before replay. SwiftDataCollection does not yet expose an equivalent hook.
- **Idempotency ergonomics:** TanStack passes an explicit `idempotencyKey` into mutation functions. SwiftDataCollection handlers can use `context.transaction.id`, but a named idempotency-key convenience is still open.

## Changelog

### Latest Status

- Core offline transaction behavior is now close to TanStack offline-transactions behavior where it maps cleanly to SwiftData:
  - durable transaction-first outbox
  - optimistic writes into SwiftData
  - restart replay
  - retryable failure backoff
  - network-aware pause/resume
  - reconnect-triggered retry reset
  - explicit non-retriable failure signaling
- The main intentional differences are still SwiftData/Electric-specific:
  - Electric completion can wait for observed txids or refresh confirmation instead of handler success alone.
  - Permanent failures remain visible as conflicted local state rather than being removed from the outbox immediately.
  - Scheduling remains per collection until a store-level scheduler is designed.

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
- Added same-key predecessor blocking and pending successor update compaction during dispatch.
- Deferred physical delete until authoritative sync completion so failed deletes remain visible and recoverable.
- Added autonomous replay and retry scheduling with startup, refresh, shape-apply, and foreground wake-up triggers.
- Clamped retry delays and prevented attempt count overflow.
- Added network-aware offline pause/resume via injectable connectivity monitoring and an `NWPathMonitor` default.
- Added immediate retry eligibility on reconnect for failed transactions.
- Added `CollectionNonRetriableError` for permanent handler failures that should mark local state conflicted instead of retrying.
- Rebuild pending local row-visible state from the durable outbox during collection bootstrap.
- Hard-enforced the one managed shape or collection per model type rule within `SwiftDataCollectionStore`.
- Split diagnostics into filtered levels, app logger, OSLog tracer, and handler-based capture.
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
  - retry saturation and delay bounds
  - diagnostics payload filtering
  - same-key blocking and compaction
  - offline queueing and reconnect retry behavior

### Current Known Constraints

- SwiftData sync currently enforces one managed shape or collection per model type within a `SwiftDataCollectionStore`.
- The automated confidence bar is restart-grade persistence coverage plus high-fidelity protocol-contract tests, not a full live backend E2E lane.
- Dynamic headers and parameters are not yet supported.
- Postgres coercion coverage is still incomplete.
- Dispatch scheduling is per collection, not globally FIFO across all collections.
- Public outbox administration APIs are not yet exposed.
- Mutation handlers do not yet receive a named idempotency-key field; use the transaction ID when idempotency is required.
- There is no `beforeRetry`-style hook for apps to filter or drop loaded transactions before replay.
- Resolved/conflicted outbox retention is not yet bounded by a cleanup policy.
