---
cairn: tasks
change: staged-delete-and-move
---

- [ ] `absorb_upsert`: honor a `Tombstone`-status upsert (mark deleted, keep
      binding, no resurrect/adopt); live upserts unchanged
- [ ] `ReplicaMutation::Move`: add `placeholder`; stage source tombstone + target
      `Created` placement (origin = source), like `Copy`
- [ ] Update the `Move` doc comment and the `move_tombstones_source_with_target`
      test to assert both ops
- [ ] hub test: absorbing a `Tombstone` upsert marks the item deleted, keeps the
      binding, and projects a `Tombstone` (pushes the remove)
- [ ] himalaya `move_messages`: supply the target `placeholder`
- [ ] io-pimdir round-trip test: a staged Remove drops the item from
      `list_items` (kept binding); a staged Move empties the source and fills the
      target
- [ ] `cargo test` green across io-replica, io-pimdir; `cargo build` himalaya
- [ ] Fold delta into `cairn/spec/hub.md` + `cairn/spec/mutate.md`; write log
