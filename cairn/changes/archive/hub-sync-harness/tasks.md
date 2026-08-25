---
cairn: tasks
change: hub-sync-harness
---

# Tasks

- [x] A shared `HubStore` (hub, residual rows, objects, per-source checkpoints) and a per-source `ReplicaStorage` view of it.
- [x] Two clients over it, each with its own remote and its own options; a `quiesce` that syncs and hydrates until the hub stops changing.
- [x] Scenarios: mirror, flag propagation, delete propagation, a source refusing removes, a read-only source, the pending-create projection, and parity with the plain store.
- [x] The push-result rules the chunked drain rests on: an unreported push stays pending; results are matched by handle, so a short, duplicated, out-of-order or unknown-handle set changes nothing.
- [x] Fold what the harness proved into the spec: a hub-backed store owns the rows the hub cannot key, and mirroring is a sync plus an upgrade.
- [x] Pin the delete-policy interaction it surfaced, and say in `ReplicaDeletePolicy` that a hub-bound source wants `Keep`.
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] Log entry; mark landed.
