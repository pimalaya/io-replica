---
cairn: change
id: engine-algorithm-audit
status: landed
created: 2026-08-25
---

# What a first reader finds wrong in the engine

## Why

This crate grew one change at a time, each reviewed against the state before it and none against the whole. On 2026-08-25 every file under `src/`, the tests, the examples and the cairn spec were read cold, in full, by a reader with no history of the design and no stake in defending it. Four of the findings below were reproduced with throwaway integration tests against this repository by path; the rest are read from the source and marked where they are not certain.

Nothing here is agreed. It is a triage list: each item that survives review becomes its own change with its own delta. Two of them contradict each other about which mechanism is supposed to survive, so the order matters.

## What

### `Move` delivers the item to the target twice

`ReplicaMutation::Move` (`src/mutate.rs:250`) stages a `Created` placement in the target carrying the *source* as its origin, exactly like `Copy`, and tombstones the source carrying the *target* as its origin. `sync` reads that tombstone's origin as a destination and derives `ReplicaChange::Remove { to: Some(target) }` (`src/sync.rs:445`), a server-side move, which physically delivers the item. The target's own sync independently derives `Add { origin: source }`, a server-side copy, which delivers it again.

Reproduced: seed one message in `inbox`, move it to `archive`, sync `archive` then `inbox`, and `archive` holds both `i1-copy` and `i1-moved`. In the other order it is not a duplicate but a permanent zombie: the add's origin is already gone, the copy is rejected on every run, and a `Created` placeholder lingers in the target forever. `tests/property.rs:834` codifies that lingering as expected, which is how it survived.

The `staged-delete-and-move` change turned `Move` into copy-and-remove and never cleared the tombstone's destination. Both mechanisms are still wired, and `src/change.rs:56` still describes `Remove { to }` as the move, so the first decision is which one survives. If it is copy-and-remove, the source tombstone stages with no origin, and the target add has to land before the source remove, which nothing enforces today.

### A read-only source's local delete is lost forever under delta sync

`src/sync.rs:402` drops the placement outright when `push` is false. The option's own documentation argues that the delete cannot propagate, so the replica mirrors the source and the next enumerate re-adds the member. That holds only for a complete snapshot. An incremental enumerate never lists the handle, since nothing changed upstream, and `delta_candidates` only revisits local handles that are not `Clean`, which a dropped row is not. Reproduced over five successive delta syncs: the item stays on the server and stays absent from the replica.

The two "cannot push a delete" paths are also semantically opposite: `push = false` mirrors the delete, `rights.remove = false` keeps it pending. `ReplicaPushRights::none()` is therefore not equivalent to `push = false`, which nothing says.

### The hub collapses two placements sharing a link id

`src/hub.rs:314` keys logical items by `ReplicaLinkId` and holds one binding per source. Two copies of the same `Message-ID` in one mailbox, which is routine for a self-sent message with Bcc, a resend, or a message filed twice, collapse to one, last write wins. Reproduced by absorbing upserts for two handles carrying the same link and projecting.

The consequence is worse than a lost row. `load` projects one handle, so the sync sees the other as absent and pulls it back, and the following `absorb` overwrites the binding with that handle, hiding the first again. A hub-backed store oscillates between the two, writing on every sync, never quiescent, which breaks the idempotence the whole design rests on. It is also the reason io-pimdir carries a bolt-on residual list for link-less placements.

### `KeepBoth` stages a duplicate nothing can identify

`src/sync.rs:680` mints the copy with `link_id: None`, while `src/change.rs:36` states that the link id *is* the idempotency key a consumer uses to detect that a retried add already landed. A crash between the serviced push and the recording write therefore appends the body twice with nothing to detect it, and every other create path carries a link. In hub mode `absorb_upsert` returns early on a link-less placement (`src/hub.rs:309`), so the copy is silently discarded from the shared view, which is exactly the version `KeepBoth` exists to preserve. The synthetic handle suffix is a constant, so a second `KeepBoth` on the same handle before the first is pushed overwrites it.

### `DropPlacement` means four things and the hub reads one

`absorb_drop` (`src/hub.rs:443`) treats every drop of a bound member as a genuine delete and propagates it to every other source. The op is emitted for a confirmed upstream expunge, for a local-only delete on a read-only source, for an accepted local remove, and for a `rekey_create` placeholder cleanup. The read-only case is the dangerous one: the sync promises the delete applies to the replica only, and the hub turns it into a tombstone on the writable source, which pushes a real remove. A read-only source can destroy data on a writable one.

The write op should say why: a reason on `DropPlacement`, with the hub marking the item deleted only for an upstream or confirmed drop.

### The storage seam has no scoped read

`WantsLoad` carries a collection and nothing else (`src/coroutine.rs:100`). `ReplicaMutate` loads every placement of the collection to find one handle (`src/mutate.rs:341`) and scans them all for a link collision on `Add`; `ReplicaUpgrade` loads all of them to touch a handful. Against the reference store it is worse than linear, because io-pimdir's `load` materialises the whole hub and projects every item, and its `write` loads the hub again, clones it, absorbs and diffs. Toggling one flag on a message in a 100k mailbox is around four full-collection materialisations.

A scope on the load, all handles or a named set or a link, costs one enum and is the highest-leverage change in either crate. Only `sync` and `rekey` need the whole collection.

### The full merge clones where it could join

`full_candidates` (`src/sync.rs:301`) builds a set union of both key spaces, cloning every handle, then clones every remote item; `merge` clones the whole placement per candidate, and `pull_flags`, `rebase`, `pull_content` and `mark_conflict` each clone it again before pushing into the write batch. Both sides are already sorted, so this is a two-pointer merge-join written as a set build. A 100k full sync allocates on the order of 300k placement and handle clones and holds the entire local map and an unbounded write vector at once.

Related: `absorb_drop` scans every hub item per drop, so absorbing a purge of k deletes over n items is O(k*n). An index from source and handle to link fixes it.

### A content push suppresses the flag merge for that item

`src/sync.rs:511` returns the content change and never reaches `reconcile_flags`, three lines below a comment arguing the opposite for the conflict case, that a delta lists an item exactly once so skipping the flag merge would lose it for good. It self-heals only because the checkpoint recorded is the pre-push one, so the next delta re-lists the item. The cost is an extra round trip and a concurrent remote flag change that is invisible to the caller in the meantime, with no event emitted. Returning both changes means keying the pending maps by handle and kind, since push results are matched by handle alone.

### `rekey` depends on batch ordering that nothing promises

`src/rekey.rs:114` emits every `DropPlacement` first and every `UpsertPlacement` after. Through the hub each drop marks the item deleted and removes the binding, and only the later upsert clears it, so correctness rests entirely on the storage applying ops in list order, which `ReplicaStorage::write` does not require: it promises atomicity and nothing else. A storage that groups by op kind turns a UIDVALIDITY bump into a cross-source mass delete. It matters concretely when the new handle space overlaps the old one, which is the common case. Emitting a drop only for genuinely unmatched old handles removes the dependence, and also removes the accidental coupling with the drop-reason finding above.

### `ReplicaStatus` flattens two orthogonal axes

`{Clean, Dirty, Tombstone, Conflict, Created}` (`src/placement.rs:217`) conflates membership intent with sync state, and every special case in the crate is a symptom: `SetFlags` must not clobber `Created` or `Conflict`; `Edit` must not clobber `Created`; `pull_flags` and `rebase` special-case `Conflict`; `rebase_content` special-cases `Tombstone`; `reconcile_flags` needs a content-edit sub-case only to decide whether `Dirty` is a no-op; `conflict_revision` is a field meaningful under one status; and the merge cannot ask whether there is a staged content edit, so it re-derives `object != base.object` in six places with four different status guards and two opposite readings of a missing base (`src/sync.rs:413`, `468`, `601`, `737`, `src/rekey.rs:149`, `197`).

Splitting it into a membership (member, pending add with its origin, pending remove with its destination), a flag-dirty bit and a content state (synced, edited, diverged with the observed revision) deletes all seven special cases, folds `origin` and `conflict_revision` into the variants that own them, and makes illegal states unrepresentable, including a tombstone whose origin means destination rather than source, which is the root cause of the `Move` duplication.

### Crash safety is all-or-nothing per run

The entire run, every pull, rebase, drop, rekey and the checkpoint, is one `WantsWrite` (`src/sync.rs:1069`). A crash after the pushes were serviced replays every push rather than the tail, and `src/change.rs:20` pushes idempotency onto the consumer without giving it a stable operation id to log; only adds carry a link id, and `KeepBoth` adds do not. Chunking pushes with their recording writes, and deriving an idempotency key for every change, bounds the blast radius.

### A body-less `Full` row never heals outside the hub

`src/upgrade.rs:86` skips a placement already at `Full` regardless of whether it holds a body. The 0.4.1 change fixed it for hub-backed stores by projecting the stored level, and 0.4.2 records that the plain path still needs a resync. The invariant is cheap to enforce here: revisit at `Full` when the object is missing, and at `Meta` when the meta is.

### Smaller

- `collection::ReplicaCollection` (`src/collection.rs:36`) is referenced nowhere in this repository, its tests, its examples or io-pimdir. Its `enumerated` flag is the "the probed spine is complete" invariant the crate header calls load-bearing, and the algorithm models it nowhere: completeness comes off the consumer's snapshot on every run.
- `stage_conflict_dup`'s missing-object early return (`src/sync.rs:682`) is unreachable, since resolution only runs when the content changed locally.
- Resuming a completed coroutine returns a default report rather than an error (`src/sync.rs:1076`); the property test only checks that it does not panic.
- `may_push` (`src/sync.rs:138`) takes a closure to read a bool field, at five call sites where the field would be shorter.
- `report.pushed` counts every accepted result even when no pending entry matched (`src/sync.rs:1013`), so a duplicate result from a consumer inflates it.
- `Remove { to: None }` is documented as "the consumer routes it to trash" (`src/change.rs:56`), which is product policy on a protocol seam. The engine should say what it means and let the consumer's configuration decide.

### On the policy matrix

There is no offline, backup, mirror, migrate or two-way policy in this crate: there is `ReplicaSyncOptions { push, rights, conflict, full }` and `ReplicaHubConflict`, which is the same algorithm with switches, and that is the right call. The one axis where the switches are not orthogonal is deletion, which is what the two delete findings above are, and it is why soft deletion exists as a storage-side workaround. The disposition of a local delete deserves to be its own option, mirror or keep-pending or retain, instead of an emergent property of two unrelated switches.

## Compaction

Around 300 lines of `src/`, roughly a quarter of the non-test source, plus about 80 lines of tests.

- Delete `ReplicaCollection`, unreferenced.
- Delete `open.rs`: a state machine, an error enum and five tests to service one `WantsLoad`, where the client can call the storage directly. The crate header already concedes it exists for symmetry. 71 source lines and 82 test lines.
- The seven single-field string newtypes each repeat `as_str` and two conversions verbatim; one macro replaces about 110 lines with 30.
- The four copy-pasted argument error enums become one, with `mutate` keeping its two real variants. About 45 lines.
- One `staged_edit` method replaces the six hand-rolled predicates. About 30 lines.
- One placement builder replaces three near-identical constructors in the hub, twelve field assignments each. About 35 lines.
- The status split removes seven special-case branches, about 50 lines net.

What earns its keep and should not be touched: the coroutine trait with its five implementations and one driver, the wants-and-args seam, `ContentOutcome`, and the inline-or-persisted fetched body. The genericity is modest and correct: a caller is bound on two associated error types and nothing else, and all three parameters of the client error are load-bearing.

## Missing adversarial tests

The property suite is genuinely good, with crash injection, delta against full equivalence, two concurrent replicas and an intent ledger. What it never exercises:

- `Move` end to end, asserting the target holds exactly one copy, in both sync orders. The ledger asserts "landed in the target or still in the source", which passes with two copies.
- A local delete on a read-only source under a delta enumerate.
- Two placements sharing a link id in one collection.
- The hub driven by the real sync engine. The hub tests are pure project and absorb units over hand-built writes, and the two-replica test uses two independent replicas rather than the hub, so nothing exercises project, sync, absorb, project convergence, which is where the two hub findings live.
- `KeepBoth`, `PreferLocal` and `PreferRemote` end to end, and under crash injection. The property model only ever runs `Manual`.
- A rekey while a hub is bound, and a rekey whose new handle space overlaps the old one.
- A push result set that is short, out of order, duplicated, or names an unknown handle. The rule that an unreported push stays pending has no test.
- Rights combinations under the hub, such as one source refusing removes while another deletes.
- A write batch applied in a different order than emitted, which should either break loudly or become contractual.

## Open questions

- Is `Move` a server-side move or a copy and a remove? Both are implemented and documented, and the answer decides the fix.
- What does `DropPlacement` mean? The storage spec calls it the retention decision point, the hub reads it as a genuine delete, and the sync emits it for a local-only delete and for a placeholder cleanup.
- Is the write batch order-significant? Load-bearing in `rekey`, unstated in the storage contract.
- Must a link id be unique per collection and source? The hub assumes so, `mutate::Add` assumes so for live rows, nothing enforces it, and mail violates it routinely.
- Was `enumerated` ever meant to be enforced? The safety argument for distinguishing a delete from an uncached item rests on a complete probed spine, and a consumer that reports a partial listing as complete mass-deletes the replica, and through the hub, the other source too.
- Why does `pull_flags` fabricate a base with no object for a base-less placement (`src/sync.rs:717`) while the placement holds a body? That is exactly the shape 0.4.2 calls "reads dirty forever", and io-pimdir's base-presence finding may be the same bug from the other end.
- Is `full` a per-run flag or recovery state? Nothing records that a full sync is needed after a suspected drift or a rejected delta, so recovery is the caller's problem with no signal from the engine.

## Scope / non-goals

- This change lands no edit. Accepted findings each get their own change, delta and log entry.
- Storage-side and format-side findings live in io-pimdir under `store-algorithm-audit` and in pimdir under `spec-algorithm-audit`, and are not repeated here.
