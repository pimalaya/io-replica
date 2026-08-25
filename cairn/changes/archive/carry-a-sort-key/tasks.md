---
cairn: tasks
change: carry-a-sort-key
---

# Tasks

- [x] Add `ReplicaSortKey` and carry it on `ReplicaPlacement` and
      `ReplicaFetchedItem`.
- [x] Thread it through `hub` (absorb and project), `upgrade`, `mutate` and
      `rekey`, wherever `meta` is already carried.
- [x] Emit it on `ReplicaWriteOp::UpsertPlacement`, which falls out of it
      riding the placement.
- [x] `Edit` takes an optional key on the same terms as its optional `meta`;
      `Add` takes one outright.
- [x] Refresh the key at every fetch tier, unlike the link id, since it is a
      projection of content rather than an identity.
- [x] An unknown key must not erase a known one in the hub, mirroring the
      absent-summary rule.
- [x] A rekey falls back to the old placement's key when the meta fetch
      resolved none.
- [x] Tests: a key round-trips through the hub; an unknown key does not erase a
      known one; a later derivation replaces an earlier one.
- [x] CHANGELOG (breaking).
- [x] Fold `delta.md`; log; land.
- [ ] **Downstream, io-pimdir**: bind the field on insert and update, so the key
      rides the ordinary write and the restating pass can go.
- [ ] **Downstream, neverest and himalaya**: update on their own schedule; they
      compile against 0.3 until then.
