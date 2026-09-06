# Roadmap

This roadmap tracks planned package work by version. See `CHANGELOG.md` for released changes.

The package is intentionally split into two deliverables:

1. `SwiftDataCollection`
   The SwiftData-first collection core: optimistic local mutations, transaction-first durable outbox state, replay, retry, lifecycle, and backend-neutral row materialization.
2. `ElectricSwiftDataCollection`
   The Electric adapter: Electric shape subscriptions, Electric row conversion, SwiftData materialization, and txid-backed authoritative completion. It depends on `ElectricSwift` v0.1.0 from GitHub.

## Current Status

Latest released version: `v0.1.2`.

`main` contains unreleased `v0.2.0`-targeted hardening work: durable staged inserts, serialized managed writes, retry bounds, network-aware offline pause/resume, reconnect retry reset, and coordinated permanent-refusal inspection and repair.

The collection path is restart-grade and offline-aware. Transaction durability is transaction-first on disk, optimistic deletes are recoverable, retry/replay is autonomous, network loss pauses outbound dispatch, reconnect resumes eligible work, one managed shape or collection per model type is enforced, and the release-grade evidence bar is in place.

The Electric adapter contains the old read-side SwiftData sync pieces, so there is no separate `ElectricSwiftData` module in this package.

## v0.2.0

### Release Goal

Ship the offline transaction hardening already on `main` together with coordinated conflict inspection and repair, including its schema migration and consumer upgrade guidance. Decide the next scheduling API boundary.

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
- Per-collection `CollectionDispatchWait`, defaulting to the existing `.dispatchAttempted` behaviour, plus a public `flush()` for explicit drains.
- Private per-dirty-key authoritative evidence and deterministic overlay materialization shared by the coordinator, Electric, and Fetch without adding a UI read/query layer.
- Monotonic transaction sequencing, stable compacted dispatch groups, frozen retry requests, and same-key conflict barriers.
- Public conflict snapshots/update streams and atomic group discard with successor payload repair.
- A schema-composition helper and startup validation for runtime metadata; v0.2.0 uses an explicit hard schema/source migration.
- Distinct row states: `.error` for retryable failures and `.conflicted` for permanently refused intent.
- Canonical Electric key normalization keeps backend UUID spelling differences from breaking row materialization or txid reconciliation.

### Planned Decisions

- Decide whether `.durablyQueued` should become the default `CollectionDispatchWait`, and in which version.
  Awaiting dispatch inside a write is arguably at odds with the package's local-first stance: on a slow
  network it turns an offline-capable local write into a stall. It is also observable behaviour that
  callers depend on today -- post-dispatch state such as `awaiting` and `conflicted` is readable the
  moment a write returns, and the package's own tests assert exactly that. Flipping the default is
  therefore a breaking change that requires migrating those tests onto `flush()` or
  `CollectionTransaction.wait()`, and it belongs in a deliberate major version rather than alongside
  other work.
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

- Scope a `beforeRetry`-style hook against the v0.2.0 repair and frozen-request contracts. Dropping work must use materialization; already-submitted request bodies must remain stable.
- Scope durable retention for resolved/discarded delivery records. Conflicted work requires explicit resolution; age alone must not delete unresolved intent or its base.
- Use the v0.2.0 conflict inspection/discard interface as the first outbox administration surface. Defer generic removal/clear operations until they can share its atomic repair path.

### Diagnostics And Status

- Add a programmatic debug/trace event stream in addition to log sinks.
- Expose first-class status snapshots such as last sync age, current replay state, current connectivity state, and pending outbox counts.

### Adapter And Type Ergonomics

- Support deferred or async resolution of request headers and extra parameters.
- Expand schema-driven coercion beyond the currently implemented common scalars, arrays, and JSON shapes.
- Thread `ElectricCollectionSyncUtilities` through richer handler contexts if the public API needs it, while keeping Electric-specific utilities out of `SwiftDataCollection`.
- Define revision or ordering-evidence semantics before considering a general authoritative direct-write API.

## Backlog

### Concurrent-Edit Detection

- Scope backend-neutral expected-version/conditional-write semantics with concrete application cases, including how handlers report rejection and adapters provide authoritative evidence.
- Keep this distinct from v0.2.0 refusal repair. The package currently does not detect concurrent edits, and Electric patches do not determine the application's outbound conflict policy.
- Define merge/rebase behavior only after version and evidence contracts exist; reuse conflict inspection and repair where appropriate.

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

Private base metadata retained only for unresolved keys is permitted as write-coordination state. It must not expose a collection read/query interface or grow into a general row cache; base, outbox, and visible-row transitions must commit atomically.

### Current TanStack Offline-Transaction Diffs

The Swift implementation should match TanStack's behavior where it maps cleanly to SwiftData, but these differences are intentional or still open:

- **Authoritative completion:** TanStack removes an outbox transaction when the mutation function succeeds. Electric-backed collections may remain `.awaiting` until observed txids or refresh completion prove that SwiftData has seen the authoritative write.
- **Permanent failures:** TanStack's `NonRetriableError` removes the transaction from the outbox and rejects waiters. SwiftDataCollection parks transaction and mutation state as `.conflicted`, exposes the conflict group for inspection, and leaves its intent materialized until explicit discard.
- **Scheduling scope:** TanStack schedules through one global executor. SwiftDataCollection schedules per collection today; `v0.2.0` should decide whether a store-level scheduler or explicit dependency model is the right SwiftData adaptation.
- **Storage and leadership:** TanStack needs IndexedDB/localStorage fallback and Web Locks/BroadcastChannel leader election. SwiftDataCollection uses SwiftData durability and does not have browser tab leadership.
- **Outbox administration:** TanStack exposes broad outbox inspection and removal APIs. SwiftDataCollection intentionally exposes only conflict-group inspection and safe discard today; generic administration remains deferred.
- **Retry filtering:** TanStack has `beforeRetry` to filter loaded transactions before replay. SwiftDataCollection does not yet expose an equivalent hook.
- **Idempotency ergonomics:** TanStack passes an explicit `idempotencyKey` into mutation functions. SwiftDataCollection handlers can use `context.transaction.id`, but a named idempotency-key convenience is still open.

## Current Known Constraints

- SwiftData sync currently enforces one managed shape or collection per model type within a `SwiftDataCollectionStore`.
- The automated confidence bar is restart-grade persistence coverage plus high-fidelity protocol-contract tests, not a full live backend E2E lane.
- Dynamic headers and parameters are not yet supported.
- Postgres coercion coverage is still incomplete.
- Dispatch scheduling is per collection, not globally FIFO across all collections.
- General pending/running outbox administration is not exposed; only conflict inspection and discard are public.
- Mutation handlers do not yet receive a named idempotency-key field; use the transaction ID when idempotency is required.
- There is no `beforeRetry`-style hook for apps to filter or drop loaded transactions before replay.
- Resolved/conflicted outbox retention is not yet bounded by a cleanup policy.
- General authoritative direct writes are not exposed; staged rows become synchronized only through their managed adapter.
- Concurrent standalone Electric row application and managed collection writes over the same model type are unsupported.
