---
cairn: log
date: 2026-08-25
change: hub-sync-harness
---

# The hub, driven by the engine rather than by hand

`grep -rn hub tests/` used to return nothing. The hub's own tests are project and absorb units over hand-built writes, so the loop the module exists for had never run: project a source's view, let a real sync merge and push it, absorb what that sync wrote, project again. `tests/hub.rs` now does exactly that, with two sources over one shared store, each with its own server and its own options, driven through `ReplicaClient`.

Eight scenarios pass: a member one source holds is mirrored to the other and ends as one item bound to both; a flag change crosses; a delete crosses and the hub prunes the item once no source holds it; a source refusing removes keeps its copy; a read-only source is never appended to; the projection offers a pending create carrying the body; and one source over the hub reports exactly what the plain store reports, so a difference in the other tests is the hub's.

## What it found on the way

**A hub-backed store cannot hold what a sync pulls.** The first harness projected the hub and nothing else, and every test failed with an empty mirror. `absorb_upsert` drops a placement carrying no link id, and *every row a sync pulls is one*: an enumeration yields handles, and the link id lands on the first meta fetch. So a store that is only a hub forgets every member it pulls, and an incremental enumeration never lists them again. io-pimdir's residual list, which the audit noted as a bolt-on, is not a bolt-on at all: it is required, and the spec now says so.

**Mirroring is a sync plus an upgrade.** The hub offers a member to a source that lacks it only when it already holds the body, so it never triggers a fetch. A consumer that syncs and never hydrates mirrors nothing, and neither the module header nor the spec said it.

**The new delete policy and the hub disagree, coherently.** With `Revert` (the default), a source that refuses removes writes back a reverted placement, which `absorb_upsert` reads as add-beats-delete: the deletion is cleared for every source, and the item is mirrored back to the source it was deleted on. With `Keep`, the deletion stands and the refusing source simply holds its copy.

Both readings are defensible, which is why this is pinned by two tests rather than fixed. Reverting says "this source still holds the member", and through a hub that genuinely is a statement about the item. `ReplicaDeletePolicy::Keep` now documents that a hub-bound source wants it, and the sync spec says the same.

## Verification

- 216 tests green, `cargo clippy --all-targets` clean, `cargo fmt`.
- The harness models a real consumer rather than a convenient one: the residual rows, the per-source checkpoints, the hydration pass, and one shared object store.

Capabilities moved: `hub`.

## Still not covered

A rekey while a hub is bound, and the conflict policies end to end. Both now have a harness to be written against.
