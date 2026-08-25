---
cairn: tasks
change: engine-compaction
---

# Tasks

- [x] `ReplicaArgError` in `coroutine`, with `ReplicaOpen`, `ReplicaUpgrade`, `ReplicaRekey` and `ReplicaSync` returning it and their four enums deleted.
- [x] `ReplicaMutateError` keeps its three real variants and composes `ReplicaArgError`.
- [x] `ReplicaHubItem::project`: the shared content of a projection once, the three callers settling status, base, revision and handle.
- [x] Every coroutine holds a terminal state and errors on a resume after completion, as `ReplicaSync` already does.
- [x] Tests: one per verb for the resume-after-complete rule; the existing arg-error tests carry over to the shared type.
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] CHANGELOG (breaking: four error types removed, one added); `cairn/spec/coroutine.md` for the contract the arg error and the terminal state belong to; log entry; mark landed.
- [x] Tick the matching lines in `engine-algorithm-audit/tasks.md`.
