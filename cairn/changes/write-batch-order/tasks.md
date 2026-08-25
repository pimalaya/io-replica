---
cairn: tasks
change: write-batch-order
---

# Tasks

- [x] Tests first, both watched failing: a reused handle, and a resurrected edit, are not dropped by the batch that writes them.
- [x] `rekey` tracks the handles its upserts write and drops only the rest; the drops move after the upserts, which no longer matters.
- [x] State the ordering contract on `ReplicaStorage::write` and in `cairn/spec/storage.md`, including the sync pair that relies on it.
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] CHANGELOG under `### Fixed`; fold `delta.md`; log entry; mark landed.
