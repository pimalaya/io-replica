---
cairn: log
change: engine-algorithm-audit
date: 2026-08-25
---

# The cold-eye audit's correctness findings landed

A first-time reading of the whole crate (`engine-algorithm-audit`) produced a triage list of correctness bugs, shape problems and compaction. The correctness half landed; the two large refactors and the identity redesign did not, and are recorded below with the reason.

Every fix went in test-first: the regression test was written against the old code, watched fail, and only then made to pass. Two of them changed the design once the tests spoke.

## What landed

- **A move delivered the item to the target twice** (capabilities `sync`, `mutate`). `ReplicaMutation::Move` stages both halves, and both could deliver: the target's create by copying from its origin, the source's tombstone by relocating the member into its destination. Synced target-first, the server ended up holding the copy *and* the relocation. `ReplicaChange::Remove` now carries the `link_id` its destination would receive, and a consumer relocates only while the destination does not already hold it. An item whose link id is not resolved has no such key, so only the source half is staged for it.

  The first attempt dropped the source's destination instead, making the remove a plain delete. The property model rejected it within seconds: synced source-first, that deletes a never-fetched item before its copy can run, trading a duplicate for an unrecoverable loss. The two halves recognising each other is the only shape that is safe in both orders.

- **A read-only source's local delete was lost for good under a delta enumerate** (capability `sync`). The delete was applied locally on the promise that "the next enumerate re-adds the member", which holds only for a complete snapshot: an incremental one never lists an untouched member again, and the merge only revisits local rows that are not clean, which a dropped row is not. The tombstone is now reverted instead of applied, which also keeps the placement's cached body rather than dropping and refetching it.

- **A housekeeping drop propagated as a delete to every other source** (capabilities `storage`, `hub`). `DropPlacement` was emitted for four unrelated reasons and the hub read all of them as "the item is gone", so reconciling a placeholder to its assigned handle, or rebuilding a spine after a UIDVALIDITY bump, marked the shared item deleted and pushed a `Remove` to sources nobody had touched. The op now carries `ReplicaDropReason`, and only `Deleted` propagates. This also removes rekey's dependence on the write batch being applied in list order, which the storage contract never promised.

- **A `Full` row holding no body was skipped for ever** (capability `sync`). 0.4.1 fixed this for hub-backed stores by projecting `stored_level`; the plain path still needed a resync. The upgrade now revisits a placement whose level claims a tier it does not hold, on both rungs. The test that guarded the old behaviour was itself built on a body-less `Full` placement, which is why it passed.

- **A keep-both duplicate could not be identified** (capability `sync`). It was staged with no link id and a constant handle suffix, so a retried add had no idempotency key, a storage sharing items by link could not hold it, and a second resolution overwrote the first. Both are now derived from the forked body, which is the duplicate's actual identity: it is a new item, not another copy of the one it forked from.

- **A content push suppressed the flag merge for that item** (capability `sync`). It self-healed only because the recorded checkpoint is the pre-push one, so the next delta re-listed the item; until then a concurrent remote flag change was invisible and no event fired. The flag axis now always runs and only withholds its own *push*, so one handle still yields at most one change.

- **A load now states what it needs** (capability `storage`). `WantsLoad` carries a `ReplicaLoadScope`, and a mutation reads the one placement it edits (an `Add`, the rows holding its link id) instead of the whole collection. The scope is a floor, so a storage that ignores it stays correct. The in-memory test storage honours it *exactly*, which is what proves the verbs no longer need more.

- **Smaller**: `ReplicaSyncReport::pushed` counts changes this run derived rather than results the consumer reported; `ReplicaSyncOptions::may_push` took a closure to read a bool field and is inlined; `ReplicaPlacement::staged_edit` replaces six hand-rolled "is there a local content edit" predicates that disagreed in four ways; `ReplicaCollection` is deleted, having been referenced nowhere in this repository, its tests, its examples or io-pimdir.

## Not landed, and why

- **The hub collapses two placements sharing a link id in one collection.** Real (two copies of one `Message-ID` in one mailbox collapse, and a hub-backed store then oscillates between the two handles, writing on every sync). Not a patch: the hub keys logical items by link id and holds one binding per source, so the fix is an identity change, and two copies in one collection are not one item for flags either. It needs its own change, plus a `bindings` key change in io-pimdir.

- **`ReplicaStatus` flattens membership intent and sync state.** The analysis holds and the seven special cases are real, but it is a public-API redesign touching every verb, and it is worth doing against the regression tests this change added rather than alongside them.

- **Chunked pushes and a per-change idempotency key.** The crash window is still the whole batch. Chunking changes the coroutine protocol and the key changes `ReplicaChange`; both want their own change.

- **`open.rs` was proposed for deletion as pure ceremony.** It is not: `pimalaya-linux` drives `ReplicaOpen` as its generic read path in four places. Kept.

## Verification

- 188 tests green, `cargo clippy --all-targets` clean, `cargo fmt`, `cargo doc` without warnings.
- New `tests/membership.rs` covers a move in both sync orders, linked and never-fetched, and a copy.
- The property suite (crash injection, delta-vs-full equivalence, two concurrent replicas, the intent ledger) is unchanged and green; it caught the first move design and is what rejected it.

## Consumer impact

Breaking, on a 0.x line: `ReplicaStorage::load` takes a scope, `ReplicaWriteOp::DropPlacement` carries a reason, `ReplicaChange::Remove` carries a link id, `ReplicaYield::WantsLoad` is a struct variant, `ReplicaCollection` is gone. io-pimdir must at minimum match the new shapes; propagating a `Superseded` drop as a delete is the one that matters for data.

Capabilities moved: `sync`, `storage`, `mutate`, `hub`.

## Addendum, same day

Two of the deferred items were revisited after `duplicate-link-id-freeze` landed, and one of them is now closed on the merits rather than on effort.

- **The `ReplicaStatus` split is withdrawn, not deferred.** The case for it was seven special cases plus six hand-rolled "is there a content edit" predicates plus the tombstone whose origin meant destination. The predicates are now one `staged_edit`, the tombstone bug was fixed where it was, and `duplicate-link-id-freeze` deliberately added `Ambiguous` as a *variant* precisely so the compiler forces every rule to say what it does with an identity the engine cannot resolve. That exhaustiveness is the safety property the freeze rests on, and splitting the enum into orthogonal axes would remove it. The remaining special cases are each a rule saying something true about one state.

- **The hub's per-drop scan is mitigated rather than fixed.** It was O(k·n) per batch; it now scans only the *scoped* hub a storage loads for its batch, so n is the batch rather than the collection wherever the storage honours the scope, which the reference one does. The index remains worth adding if a consumer ever loads whole collections.

Still open, unchanged: chunked pushes with a per-change idempotency key (the crash window is the whole batch), and the merge's set-build-plus-clone where a sorted merge-join would do.

Also landed here: the five string-newtype identities are one `replica_id!` declaration each rather than fifteen lines of identical impls, and `ReplicaLoadScope::Link` became `Links`.
