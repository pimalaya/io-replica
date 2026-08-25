---
cairn: tasks
change: merge-join
---

# Tasks

- [ ] Replace `full_candidates`' set-union with a two-pointer merge-join over the sorted local map and a sorted remote slice.
- [ ] Take `&ReplicaPlacement` through the merge; clone once, where a write takes ownership.
- [ ] Sort the remote snapshot defensively, and state the expectation on `ReplicaRemoteSnapshot`.
- [ ] Bound the write batch alongside the push chunks (`chunked-pushes`).
- [ ] Tests: the existing delta-versus-full property still holds; add one asserting the derived changes are identical before and after over a seeded collection; measure allocations or wall clock on a large synthetic collection and record the figures in the log entry.
- [ ] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [ ] CHANGELOG under `### Changed` (no API change); log entry; mark landed.
