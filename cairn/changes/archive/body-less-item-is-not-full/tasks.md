---
cairn: tasks
change: body-less-item-is-not-full
---

# Tasks

- [x] `ReplicaHubItem::stored_level`, documenting why the level and the body are
      two facts and which one is authoritative.
- [x] `absorb_upsert` records it after the content merge.
- [x] `bound_placement` and `tombstone_placement` project it, so a store written
      before this heals on its next upgrade.
- [x] Tests: a refreshed item drops to `Meta`; a `Full`-with-no-body item
      projects below `Full`; an item that has a body keeps its level.
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] Verified end to end through neverest against a live Radicale: a card
      edited on the server now reaches the store, a store already stuck heals,
      and the run after it is quiescent.
- [x] Fold `delta.md` into `cairn/spec/hub.md`; add the `cairn/log` entry; mark
      the change `landed` and archive it.
