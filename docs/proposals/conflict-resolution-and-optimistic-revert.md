# Conflict repair: final design decision

Status: implemented for the unreleased `v0.2.0`. The shared classification,
coordinated materialization, schema changes, and conflict inspection/discard
surface described below now ship together in the release branch.
Reviewed against `a07065bf654b`. Target: `v0.2.0` for conflict repair and its
schema/handler-contract changes, alongside the current hardening work. The
independently testable adapter classification correction landed first; the
remaining repair and migration gates are now implemented in the same release
scope.

## Fit with the package

Conflict repair completes the backend-neutral write coordinator's responsibility:
durable optimistic intent must remain inspectable and have a supported abandonment
path after a permanent refusal. SwiftData remains the only reactive row/query
surface. Electric remains responsible for protocol translation and authoritative
observation; the Fetch adapter must satisfy the same core materialization contract.

A base retained only for dirty keys is coordination metadata. It has no collection
query interface, secondary query engine, or cache lifetime beyond pending intent.
This is consistent with the existing prohibition on a second UI read model.
Access control helps hide this metadata; not conforming to
`SwiftDataCollectionModel` alone does not prevent a SwiftData `@Model` from being
queried. Public schema composition must make the package's hidden models
registerable without exposing their concrete types.

This work repairs refusals. It does not detect competing edits. Server version
checks, conditional writes, and merge policies require a separate backend contract;
Electric patches alone do not establish universal field-level last-writer-wins
semantics for outbound application writes.

## Corrections to the earlier drafts

- Transactions already persist `sequenceNumber`. The queue orders transactions
  by that number, with `createdAt` as a tie-breaker, and allocates max + 1.
  Mutation enumeration now uses a persisted within-transaction ordinal, with
  date and UUID only as legacy tie-breakers. The defect was inconsistent
  ordering, not the absence of durable transaction order.
- `originalRow` snapshots the visible row at record time, including earlier
  optimistic changes. It cannot establish an authoritative base.
- Recomputing a successor from an overlay that still includes a parked conflict
  does not remove the refused values. Blocking and payload repair solve different
  problems; neither substitutes for the other.
- Compaction can combine an outbound request without overwriting source intents.
  Keeping the existing outbox intents intact is not an additional row store and
  does not require sending more requests.
- A non-retriable error stops automatic retries. Current permanent failures do
  not retry forever.
- Adding persisted models changes the consumer schema. Whether a particular
  schema change can migrate automatically must be validated, not presumed.

## Core module and persisted state

Introduce one backend-neutral materialization module with internal seams for
recording intent, applying adapter evidence, acknowledging completion, and
discarding a conflict group. It owns base lifetime, ordered overlays, row-state
derivation, and rebuilding eligible outbound representations. Keep these rules
out of the coordinator and adapters.

Persist a base per (collection ID, model identity, serialized key), using an
unambiguous composite encoding for uniqueness rather than delimiter concatenation.
The base distinguishes:

- known row;
- known absence;
- unknown, for migrated dirty keys or incomplete authoritative evidence.

Accepted state from immediate completion must also be distinguishable from an
observed server representation in conflict inspection. Decode failure is an error,
never authoritative absence.

Capture the base before the first optimistic change, in the same commit as the
outbox and visible row. A new local create starts from known absence. An existing
clean managed row supplies its current baseline. Additional intents never replace
the base with a snapshot of the optimistic row.

Persist a next-transaction sequence in collection metadata. Seed it above existing
sequences and allocate/increment it inside the write commit. Reuse the existing
transaction sequence field; do not add an independent per-key clock. Since
same-key mutations coalesce within a transaction, transaction sequence orders a
key's overlays. Persist an ordinal for mutation order within a transaction where
handler grouping requires it. Dates remain diagnostics. Reject overflow.
Migration must deterministically handle legacy ties without claiming to recover
historical order that was never recorded.

The scope remains one managed writer per collection, as supported today; this
counter does not establish coordination across independent processes.

## Atomic transitions

For each transition, fetch current base, outbox, and rows in one fresh context
under the existing write gate. Save all affected keys, transaction/group state,
and any sequence change together. On failure, retain the previous durable state.
Publish conflict updates and release dispatch dependencies only after commit.

Adapters provide normalized replacement, patch, absence, and completion evidence.
The core owns persistence and row materialization. Electric checkpoint changes
must commit with their batch's base and row changes, so a restart cannot skip
evidence that failed to persist.

A network request remains outside the database commit. Persist its group,
membership, and submitted representation before invoking the handler.

## Base advancement and materialization

| Event | Base action |
|---|---|
| Authoritative replacement | Replace the base with the supplied row |
| Authoritative patch | Patch a known row; an incomplete patch cannot make an unknown base known |
| Authoritative absence | Record absence |
| Immediate create accepted | Establish the submitted create row as accepted state |
| Immediate update accepted | Apply submitted changed fields to a known row |
| Immediate delete accepted | Record absence |
| Token or refresh completion | Remove the acknowledged overlay only after applicable authoritative evidence; never apply its payload speculatively |
| Discard | Remove local intent without advancing the base |

An immediate update over absence does not implicitly establish a row. Handlers
that implement an upsert must obtain authoritative replacement/refresh evidence
before completion. The immediate contract cannot infer server-side recreation.

Immediate completion accepts the local representation. Server-generated or
transformed values require an adapter completion mode that obtains authoritative
data. These completion modes are package-internal today: public documentation must
explain the actual Electric/Fetch handler interfaces, not tell a generic public
Void-returning handler to return an inaccessible completion enum.

Build the visible row by applying surviving intent in transaction order:

- Create establishes its full intended row.
- Update patches changed fields only when a row exists.
- Local delete preserves the recoverable row and derives pending-delete state.
  Keep current soft-delete semantics: physically deleting on enqueue would break
  consumers that display pending/failed deletes.
- Authoritative absence beneath updates leaves the visible row absent while the
  outbox survives. Conflict inspection must work without a row.
- Staged local rows retain their existing lifecycle. Publishing a staged row
  enters ordinary intent tracking atomically; server resolution of staged work
  still follows the current staged contract.

When the last intent disappears, first materialize the final base, then remove
the base in the same commit. A discarded local create disappears only if the base
is still absent. A discarded delete returns the row only if authoritative state
still contains it.

Electric must-refetch and Fetch snapshot policies are part of this contract.
A reset invalidates stale evidence; snapshot completion establishes absence only
where the adapter's scope and missing-row policy warrant it. Do not interpret a
missing row in a partial snapshot as deletion.

Token acknowledgement may cover multiple keys/messages. Do not retire overlays
before their authoritative materialization or infer that seeing a token supplies
a full row. Evidence insufficient to establish a safe base remains unknown and
blocks destructive repair until refresh supplies it.

## Intent, handler payloads, and compaction

Keep recorded per-transaction intent intact. Build an outbound representation from
the current base plus ordered eligible intent, and freeze that representation for
a submitted attempt and its automatic retries. This preserves request identity
when callers use the transaction/group ID as an idempotency key.

On discard, rebuild successor representations that have never been submitted,
using each intent's original changed-field values and the corrected prefix row.
Do not derive the next patch from a payload that has already been overwritten.
`modified` is a dispatch representation; its unchanged fields may differ from
the record-time row. Document that contract. Preserve `original` as the
record-time snapshot rather than silently repurposing it as server state.

For an absent prefix, an update retains its recorded intent and diagnostic payload
but cannot claim to be a materialized full row. The handler may refuse it, or
recreate through authoritative adapter evidence. No adapter invents a permanent
refusal from absence alone.

Do not rewrite already-submitted requests during discard or retry. New same-key
successors are blocked behind conflicted predecessors, which prevents this case
for newly recorded work. Migrated histories that already contain submitted
successors require authoritative recovery before repair.

Retain outbound compaction, but stop destructively changing the leading source
mutation. Select contiguous compatible, never-submitted single-key transactions
under the existing operation restrictions. Persist the leading transaction ID as
group ID, membership, and the frozen aggregate request. A group is formed once;
never absorb new members during retry under an existing idempotency key.

Resolve/discard the group together. A standalone transaction is a one-member group;
multi-key transactions remain indivisible. Snapshotting a submitted request is
outbox delivery state, with bounded lifetime, rather than a general row cache.

This revises the earlier choice to keep lossy source-payload mutation. Grouping
alone cannot prevent replaying intermediate source effects incorrectly or changing
a retry's request body after compaction.

## Status and dispatch policy

Centralize these classifications in the core and remove adapter copies:

| State | Requires resolution | Participates in overlay | Blocks same-key successor dispatch |
|---|---|---|---|
| pending, sending, awaiting, failed | yes | yes | yes |
| conflicted | yes | yes | yes |
| resolved or discarded | no | no | no |

Discard is distinct from successful acknowledgement in durable administration.
Blocking is per key; unrelated keys continue. A multi-key transaction waits for
every predecessor affecting its keys. Automated resolution can remove a blockage;
there is no requirement that a human always intervene.

Row-state precedence: conflicted, retryable error, pendingDelete,
pendingCreate, pendingUpdate, synced. Missing rows have no row state; group
inspection carries their conflict. `.error` and `.conflicted` are distinct in
the completed v0.2.0 repair path.

## Public surface for the first release

Expose conflict snapshots and discard. Defer manual retry, custom policy
callbacks, amend, and merge execution until a concrete consumer establishes their
semantics. Automatic retry for retryable errors remains unchanged.

- `conflicts() async throws -> [CollectionConflict]`: one snapshot per group;
  storage failure must not appear as an empty result.
- `conflictUpdates`: an async throwing stream of complete snapshots, beginning
  with the current committed snapshot, with independent subscriptions and newest
  snapshot buffering.
- `discard(_ conflictID: UUID) async throws`: accepts a conflicted group ID,
  validates ownership and repair readiness, and commits group discard plus all
  affected materialization. Repetition is a no-op while the retained discarded
  group is identifiable; missing IDs and ineligible states are explicit errors.

A conflict contains its group ID, ordered transaction IDs, error, occurrence time,
repair readiness, and per-key entries. Each entry ties together operation, local
changes, and baseline evidence (unknown, absent, accepted row, observed row).
Avoid parallel keys/operations arrays and a group-wide changed-field union that
loses those relationships. Include evidence as a diagnostic snapshot, not a
separate reactive collection read interface or a guarantee of current server state.

Default behavior is park. Automatic discard is deferred with custom policies;
explicit discard plus observability is the smallest complete repair surface.
Existing failed transaction waiters stay failed after discard; abandonment is not
reported as successful server completion.

## Migration and release gates

Target the coordinated change at v0.2.0 with an explicitly breaking schema and
source migration, as approved by the maintainer. Consumers adopt the public
schema-composition helper, replace `.syncError` with `.error`, and replace
`.awaitingSync` with `.awaiting`; the persisted spellings also become `error`
and `awaiting`, with no deprecated aliases. Transparent upgrades of legacy dirty
outboxes are outside this release's supported contract.

Applications own the transition: drain/export old local work before replacing a
store, or supply their own explicit migration. The package never silently deletes
an existing store or pretends legacy snapshots establish authoritative evidence.
Schema validation rejects missing runtime metadata before starting collections.
Unknown baselines remain necessary for interrupted resets and incomplete server
evidence, and continue to prevent unsafe discard.

Implementation gates:

1. Completed: centralized adapter overlay classification with regression tests;
   see CHANGELOG for the implementation record.
2. Completed: schema registration, ordering, base states, and durable
   group/request representation.
3. Completed: core transition module integration with Electric, Fetch, immediate
   completion, staged promotion, resets, and restart.
4. Completed: conflict snapshots, discard, and distinct row conflict state.
5. Completed: hard-migration guidance and failure-injection coverage, recorded in
   CHANGELOG and README.

## Required validation

- Clock rollback and equal timestamps preserve transaction order; restart and
  retention do not reuse sequence values.
- Server patches update the base without capturing optimistic protected fields.
- Discard removes a refused field from both the visible row and never-submitted
  successor handler representations; surviving changed fields remain intact.
- Retry of an already-submitted group preserves membership and request body.
- Compacted create/update and update/update histories materialize correctly,
  including changes that return a field to its starting value.
- Immediate create/update/delete establish accepted state without clobbering
  unrelated server fields; server-generated values use authoritative completion.
- Pending and failed local deletes retain their recoverable row.
- Server absence beneath updates retains intent with no resurrected UI row.
- Multi-key discard either commits every affected key or none; injected save
  failure emits no successful conflict update or dependency release.
- Electric acknowledgement ordering, partial rows, reset/refetch, and Fetch
  missing-row policies preserve base validity and completion semantics.
- Staged promotion/resolution and base cleanup preserve their lifecycle.
- Restart restores conflicted groups and independent snapshot subscribers.
- Older schemas are rejected explicitly. v0.2.0 does not infer bases or
  compaction membership for legacy dirty stores.

## Separate follow-up: conflict detection

Scope conditional-write/version-token support independently with at least two
application cases. Define how handlers carry expected versions and return
rejections, and how adapters surface authoritative evidence. Generic merge
execution and automatic rebase are deferred. Refusal repair is useful without
this feature; conflict detection will use the same inspection/discard foundation.

## Caller integration note

The earlier Angle synchronizer-wiring finding belongs to that application's
integration. It is not part of the package repair implementation. Consumers must
use their managed collection path for mutation-aware materialization.
