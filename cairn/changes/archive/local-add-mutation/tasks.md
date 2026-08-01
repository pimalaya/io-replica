---
cairn: tasks
change: local-add-mutation
---

# Tasks

- [x] `mutate.rs`: add `ReplicaMutation::Add { handle, link_id, flags, object,
      body, meta }`.
- [x] `mutate.rs`: `handle()` returns `Option` (Add has no source handle); route
      Add through a create path (no source lookup) with a link-id collision guard
      (`ReplicaMutateError::LinkExists`).
- [x] `mutate.rs`: stage `Created`/`base:None`/`origin:None`/`level:Full` +
      `StoreObject { body: Some }`.
- [x] Tests: Add stages the append shape (Created, no base, no origin, object
      set, StoreObject present); collision guard errors; a round-trip that the
      staged create pushes as `ReplicaChange::Add { origin: None }`.
- [x] `nix develop --command cargo test`; `cargo fmt`.
- [x] Create `cairn/spec/mutate.md`; fold `delta.md`; add `cairn/log` entry;
      mark change `landed` and archive.
