---
cairn: tasks
change: merge-join
---

# Tasks

- [x] Replace `full_candidates`' set-union with a two-pointer merge-join over the sorted local map and a sorted remote slice. `delta_candidates` became the delta rule applied to a joined candidate, so both paths share the one walk.
- [x] Take the placement through the merge rather than a copy: the join owns both sides, and the merge clones once, where a write takes ownership.
- [x] Sort the remote snapshot defensively, collapse a handle listed twice, and state the expectation on `ReplicaRemoteSnapshot`.
- [x] Bound the write batch alongside the push chunks (`chunked-pushes`), cutting between candidates and never inside one.
- [x] Tests: the delta-versus-full property still holds (and caught a dropped base-less candidate); an unordered enumeration derives exactly what the sorted one derives; a handle listed twice is merged once; a merge larger than one batch hands over several with the checkpoint only in the last; a keep-both resolution is never split by a boundary. Figures measured on 100k members and recorded in the log entry.
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] CHANGELOG under `### Changed`; log entry; mark landed.
