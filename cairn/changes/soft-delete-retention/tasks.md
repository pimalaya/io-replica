---
cairn: tasks
change: soft-delete-retention
---

- [ ] Confirm a storage that hides rows from `load` is safe against the merge (no re-derivation)
- [ ] Spec: document `DropPlacement` as the retention decision point at the storage seam
- [ ] Reference: a soft-delete storage test (drop retained + hidden from load, survives resync)
- [ ] Decide whether an engine-side `on_remote_delete` option is warranted (default: no)
- [ ] Land: fold delta into spec, append log entry
