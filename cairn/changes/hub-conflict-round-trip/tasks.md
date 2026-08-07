---
cairn: tasks
change: hub-conflict-round-trip
---

# Tasks

- [x] `ReplicaSourceBinding`: add `conflicted` and `conflict_revision`, with
      docs distinguishing them from the item-level cross-source conflict.
- [x] `absorb_upsert`: record both, on the tombstone path and the live path.
- [x] `bound_placement`: project `Conflict` ahead of the Clean/Dirty decision
      and carry `conflict_revision` back.
- [x] Tests: a conflicted placement round-trips (status + revision); a later
      `Dirty` upsert clears it; a per-source conflict does not set the
      item-level cross-source `conflicted`, and vice versa.
- [x] Update the existing hub tests' binding literals.
- [x] fmt + clippy clean, whole suite green.
- [x] CHANGELOG entry (breaking struct change, minor bump).
- [x] Fold `delta.md` into `cairn/spec/hub.md`; log; land.
- [ ] **Follow-up, other repos**: io-pimdir persists the two fields (needs a
      `pimdir` schema change: `bindings.conflicted` / `bindings.conflict_revision`,
      SPEC §4.3 + §11 + `queries/bindings.sql`). Then flip neverest's canary
      assertion in `a_body_edited_on_both_sides_is_left_conflicted_not_overwritten`.
