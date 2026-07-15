# Roadmap

This roadmap tracks planned package work by version. See `CHANGELOG.md` for released changes.

The package is intentionally split into two deliverables:

1. `SwiftDataCollection`
   The SwiftData-first collection core: optimistic local mutations, transaction-first durable outbox state, replay, retry, lifecycle, and backend-neutral row materialization.
2. `ElectricSwiftDataCollection`
   The Electric adapter: Electric shape subscriptions, Electric row conversion, SwiftData materialization, and txid-backed authoritative completion. It depends on `ElectricSwift` v0.1.0 from GitHub.

## Current Status

Latest released version: `v0.1.2`.

`main` contains unreleased `v0.2.0`-targeted hardening work: durable staged inserts, serialized managed writes, retry bounds, network-aware offline pause/resume, reconnect retry reset, non-retriable error handling, and updated offline transaction parity documentation.

The collection path is restart-grade and offline-aware. Transaction durability is transaction-first on disk, optimistic deletes are recoverable, retry/replay is autonomous, network loss pauses outbound dispatch, reconnect resumes eligible work, one managed shape or collection per model type is enforced, and the release-grade evidence bar is in place.

The Electric adapter contains the old read-side SwiftData sync pieces, so there is no separate `ElectricSwiftData` module in this package.

## v0.2.0

### Release Goal

Ship the offline transaction hardening already on `main` and decide the next scheduling API boundary.

### Included From Current `main`

- Bounded retry delays and safe attempt-count saturation.
- Injectable connectivity monitoring with an `NWPathMonitor` default.
- Offline pause/resume for outbound outbox dispatch.
- Immediate retry eligibility when connectivity returns.
- `CollectionNonRetriableError` for permanent handler failures that should leave local state conflicted instead of retrying.
- Documentation of TanStack offline-transaction parity and intentional SwiftData/Electric differences.
- Durable `.stagedCreate` rows with stage, local update, publish, and discard operations.
- Per-collection serialization of managed coordinator and adapter SwiftData writes.
- Adapter-driven resolution of staged rows, with staged work preserved during deletes and resets.

### Planned Decisions

- Decide whether cross-collection ordering should be implemented as:
  - explicit transaction dependencies such as `dependsOnTransactionIDs` or `dependsOnKeys`
  - a store-level scheduler with global FIFO dispatch
  - a hybrid scheduler that only blocks declared dependencies
- Define named idempotency-key ergonomics for mutation handlers. The current fallback is `context.transaction.id`.
- Define public outbox/status inspection APIs, including pending/running counts and queued transaction summaries.

### Release Checklist

- Review new public API names and access levels.
- Run the full Swift test suite with the Xcode toolchain.
- Update README examples if any public API names change.
- Tag and publish `v0.2.0`.

## Optional v0.1.3 Patch Lane

Use `v0.1.3` only for a narrowly scoped patch that should ship before `v0.2.0`.

Appropriate `v0.1.3` work:

- critical bug fixes
- documentation corrections for released APIs
- test-only or packaging-only fixes that do not introduce new API surface

Do not use `v0.1.3` for the current offline connectivity work unless the release strategy changes; that work is targeted at `v0.2.0`.

## v0.2.x

### Retry And Outbox Operations

- Add a `beforeRetry`-style hook so apps can filter, drop, or annotate loaded transactions before replay.
- Add durable outbox cleanup and retention policy for resolved/conflicted transactions and mutations.
- Consider public outbox administration APIs such as `peekOutbox`, `removeFromOutbox`, and `clearOutbox` if they fit SwiftData-first usage.

### Diagnostics And Status

- Add a programmatic debug/trace event stream in addition to log sinks.
- Expose first-class status snapshots such as last sync age, current replay state, current connectivity state, and pending outbox counts.

### Adapter And Type Ergonomics

- Support deferred or async resolution of request headers and extra parameters.
- Expand schema-driven coercion beyond the currently implemented common scalars, arrays, and JSON shapes.
- Thread `ElectricCollectionSyncUtilities` through richer handler contexts if the public API needs it, while keeping Electric-specific utilities out of `SwiftDataCollection`.
- Define revision or ordering-evidence semantics before considering a general authoritative direct-write API.

## Backlog

### Snapshot And Subset APIs

- Add explicit snapshot and subset support where it fits the SwiftData-first model.
- Define how partial-sync or subset data interacts with SwiftData reads.

### Internal State-Machine Refinement

- Continue splitting collection runtime behavior into focused components instead of growing the coordinator.
- Prefer focused collaborators for lifecycle, dispatch, retry, txid registration, pending-state refresh, and failure handling.

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

- TanStack DB's live-query engine
- TanStack DB's index/query runtime
- a second optimistic row store layered over SwiftData
- browser-specific storage and coordination assumptions
- TypeScript-style utility bags when typed Swift facades are clearer

### Current TanStack Offline-Transaction Diffs

The Swift implementation should match TanStack's behavior where it maps cleanly to SwiftData, but these differences are intentional or still open:

- **Authoritative completion:** TanStack removes an outbox transaction when the mutation function succeeds. Electric-backed collections may remain `.awaitingSync` until observed txids or refresh completion prove that SwiftData has seen the authoritative write.
- **Permanent failures:** TanStack's `NonRetriableError` removes the transaction from the outbox and rejects waiters. SwiftDataCollection marks transaction and mutations `.conflicted` and leaves affected rows in `syncError` so the app can surface or repair local state.
- **Scheduling scope:** TanStack schedules through one global executor. SwiftDataCollection schedules per collection today; `v0.2.0` should decide whether a store-level scheduler or explicit dependency model is the right SwiftData adaptation.
- **Storage and leadership:** TanStack needs IndexedDB/localStorage fallback and Web Locks/BroadcastChannel leader election. SwiftDataCollection uses SwiftData durability and does not have browser tab leadership.
- **Outbox administration:** TanStack exposes outbox inspection and removal APIs. SwiftDataCollection has persisted outbox models and traces, but no polished public admin API yet.
- **Retry filtering:** TanStack has `beforeRetry` to filter loaded transactions before replay. SwiftDataCollection does not yet expose an equivalent hook.
- **Idempotency ergonomics:** TanStack passes an explicit `idempotencyKey` into mutation functions. SwiftDataCollection handlers can use `context.transaction.id`, but a named idempotency-key convenience is still open.

## Current Known Constraints

- SwiftData sync currently enforces one managed shape or collection per model type within a `SwiftDataCollectionStore`.
- The automated confidence bar is restart-grade persistence coverage plus high-fidelity protocol-contract tests, not a full live backend E2E lane.
- Dynamic headers and parameters are not yet supported.
- Postgres coercion coverage is still incomplete.
- Dispatch scheduling is per collection, not globally FIFO across all collections.
- Public outbox administration APIs are not yet exposed.
- Mutation handlers do not yet receive a named idempotency-key field; use the transaction ID when idempotency is required.
- There is no `beforeRetry`-style hook for apps to filter or drop loaded transactions before replay.
- Resolved/conflicted outbox retention is not yet bounded by a cleanup policy.
- General authoritative direct writes are not exposed; staged rows become synchronized only through their managed adapter.
- Concurrent standalone Electric row application and managed collection writes over the same model type are unsupported.
