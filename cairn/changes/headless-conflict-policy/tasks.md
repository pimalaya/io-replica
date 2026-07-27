---
cairn: tasks
change: headless-conflict-policy
---

- [ ] Add `OfflineConflictPolicy` enum + `conflict` field on `OfflineSyncOptions`
- [ ] Wire policy into `reconcile_content`'s conflict branches
- [ ] PreferRemote: drop local edit, pull (reuse pull_content path)
- [ ] PreferLocal: push the local body as an Update
- [ ] KeepBoth: stage a Created duplicate of the local body
- [ ] Unit tests per policy on a both-sides-edited placement
- [ ] Confirm immutable-content (no revision) never triggers a conflict
- [ ] Land: fold delta into spec, append log entry
