# Changelog

All notable package changes are tracked here by release.

## Unreleased - target v0.2.0

- Added coordinated conflict repair backed by private per-dirty-key authoritative evidence and a deterministic materializer shared by the coordinator, Electric adapter, and Fetch adapter. SwiftData application models remain the only UI read/query layer.
- Added durable conflict-group inspection through `conflicts()` and `conflictUpdates`, plus atomic `discard(_:)` that repairs visible rows and never-submitted successor payloads while preserving already-submitted request bodies.
- Persisted monotonic transaction allocation, within-transaction mutation ordering, compacted dispatch-group membership, frozen submitted requests, and conflict occurrence metadata. Same-key successors remain blocked behind parked conflicts while unrelated keys continue.
- Added `SwiftDataCollectionSchema.models(including:)` and startup schema validation for the runtime's private metadata. v0.2.0 is an explicit hard schema/source migration for older stores with dirty outboxes.
- Renamed `CollectionSyncState.syncError` to `.error` with persisted raw value `error`, and now use `.conflicted` distinctly for permanently refused intent.
- Renamed transaction and outbox `.awaitingSync` states to `.awaiting`, including the trace lifecycle state and persisted raw value.
- Corrected Electric and Fetch reconciliation so conflicted mutations remain part of the local overlay until resolved, and made authoritative server absence beneath an update remove the visible row without discarding the retained intent.
- Kept Electric reset bookkeeping limited to dirty keys while preserving evidence safely across multi-batch snapshots and restart.
- Canonicalized Electric wire keys through the collection identifier before materialization and reconciliation, including lowercase Postgres UUID keys.
- Added durable `.stagedCreate` rows with collection APIs to stage, locally update, publish, and discard existing SwiftData models without prematurely creating outbound mutations.
- Added per-managed-collection write serialization across coordinator commits, Electric batches, and Fetch snapshot application.
- Added backend-neutral staged reconciliation so adapter upserts resolve staged rows while adapter deletes and resets preserve staged work.
- Kept general authoritative direct writes deferred until cross-source ordering semantics are defined.
- Clamped retry delays and saturated transaction/mutation attempt counters to avoid overflow.
- Added network-aware offline pause/resume through injectable connectivity monitoring and an `NWPathMonitor` default.
- Pause outbound outbox dispatch while offline, keep optimistic SwiftData writes durable, and resume eligible work on reconnect.
- Made failed transactions eligible for immediate retry when connectivity returns.
- Added `CollectionNonRetriableError` for permanent handler failures that should mark local state conflicted instead of retrying.
- Added `CollectionDispatchWait` so a collection can choose whether writes return once durably queued or once outbound dispatch has been attempted. The default, `.dispatchAttempted`, preserves existing behaviour; `.durablyQueued` keeps latency-sensitive local-first writes off the network round trip.
- Exposed `SwiftDataCollection.flush()` so callers can drain pending dispatch explicitly, which `.durablyQueued` collections need in tests and shutdown paths.
- Documented current TanStack offline-transaction parity, intentional SwiftData/Electric differences, and future cross-collection scheduling work.

## v0.1.2 - 2026-07-04

- Added collection schema-aware row normalization.
- Renamed collection callbacks and flush APIs.
- Refactored Electric collection transaction reconciliation.
- Split diagnostics into app logger and OSLog tracing paths.
- Added diagnostics levels and payload filtering.
- Deferred same-key dispatch behind unresolved predecessors and compacted pending successor updates during dispatch.

## v0.1.1 - 2026-06-14

- Added date-aware collection row conversions.

## v0.1.0 - 2026-06-12

- Added SwiftData collection transaction coordination.
- Added the MIT license.
- Simplified collection row sync metadata.
- Restored the Swift package manifest and lockfile.
- Added the fetch-backed SwiftData collection adapter.
- Refined fetch adapter scope IDs and mutation reconciliation.
- Removed the Electric collection convenience overload from the public API.

## v0.1.0-beta.1 - 2026-05-23

- Initial beta release of `SwiftDataCollection`.
- Added batch apply summaries and shape-store failure reporting at the tagged commit.

Note: the GitHub release was published on 2026-05-23; the tag points at commit `2d3b6ce`, dated 2026-06-12.
