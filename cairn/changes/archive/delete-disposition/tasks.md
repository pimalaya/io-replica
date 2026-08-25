---
cairn: tasks
change: delete-disposition
---

# Tasks

- [x] `ReplicaDeletePolicy` on `ReplicaSyncOptions`, defaulting to `Revert`.
- [x] One `refuse_delete` for every path that cannot take a delete: `push = false`, a forbidden remove, and a move whose staged edit cannot ride ahead of it.
- [x] Tests: the default reverts a forbidden remove as it does a read-only delete; `Keep` holds the tombstone under both switches.
- [x] Document that a hub-bound source wants `Keep`: reverting reads as add-beats-delete across sources, which the hub harness pinned (`tests/hub.rs`).
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] CHANGELOG under `### Added` and `### Changed`; fold `delta.md` into `cairn/spec/sync.md`; log entry; mark landed.
