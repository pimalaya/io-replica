---
cairn: tasks
change: flags-unknown-state
---

# Tasks

- [x] `ReplicaFlags` becomes an enum with `Unknown` and `Known`, keeping a known-empty `Default` and the `FromIterator` shorthand.
- [x] `contains`, `is_unknown` and `known` give the state a read surface without exposing a set that may not exist.
- [x] `merge` treats an unknown side as no opinion and an unknown base as no base on this axis.
- [x] An unknown set does not erase a known one in the hub, mirroring the sort-key rule.
- [x] Tests: the merge in every unknown combination; the hub rule both ways; a sync whose local set is unknown pulls the remote one and pushes nothing.
- [x] CHANGELOG (breaking).
- [x] Fold `delta.md`; log; land.
- [ ] **Downstream, io-pimdir**: write `NULL` for an unknown set and read `NULL` back as one, which is the point of the state.
- [ ] **Downstream, consumers**: an enumeration that reports no markers (CardDAV, CalDAV) states `Unknown` rather than an empty set.
