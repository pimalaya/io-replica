---
cairn: tasks
change: chunked-pushes
---

# Tasks

- [x] `ReplicaChange` carries an idempotency key, derived deterministically from collection, handle, kind and target state; documented as the value a consumer records to recognise a replay. The four verbs became `ReplicaChangeKind`, and `ReplicaChange::new` is the only way to key one, so no change can exist unkeyed or carry a key naming something else.
- [x] `ReplicaSync` alternates `WantsPush` and `WantsWrite` over bounded chunks instead of one of each; the pending maps are drained per chunk.
- [x] The checkpoint still lands in the final write, still the pre-push one.
- [x] `ReplicaClient::run` needs no change (it already loops), confirmed and covered end to end by `tests/chunked_pushes.rs`.
- [x] Tests: a crash after chunk N replays chunk N only; the key is stable across runs for the same derived change and differs across collections, handles, kinds and target states; the existing crash property still holds (its "spurious conflict acceptable" allowance is untouched: its scenarios are one chunk long, so nothing there exercises the chunked case).
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] CHANGELOG under `### Changed` (breaking); fold `delta.md` into `cairn/spec/sync.md`; log entry; mark landed.
- [ ] io-pimdir and neverest: a push loop matches `change.kind` where it matched the change; the driver absorbs the extra push/write pairs. Hand `change.key` over once released, so each records what it applied.
