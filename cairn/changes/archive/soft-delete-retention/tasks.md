---
cairn: tasks
change: soft-delete-retention
---

- [x] Confirm a storage that hides rows from `load` is safe against the merge (no re-derivation)
- [x] Spec: document `DropPlacement` as the retention decision point at the storage seam
- [x] Reference: a soft-delete storage test (drop retained + hidden from load, survives resync)
- [x] Decide whether an engine-side `on_remote_delete` option is warranted (default: no)
- [x] Land: fold delta into spec, append log entry
