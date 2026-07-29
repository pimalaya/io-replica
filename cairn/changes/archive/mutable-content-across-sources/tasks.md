---
cairn: tasks
change: mutable-content-across-sources
---

- [x] Add `ReplicaHubConflict` (Manual / PreferIncoming / PreferExisting)
- [x] Add `conflicted` + `conflict_object` to `ReplicaHubItem`, `conflict` to `ReplicaHub`
- [x] `absorb_upsert`: detect a cross-source content divergence; keep flags element-wise
- [x] Resolve by policy: Manual flags + records, PreferIncoming adopts, PreferExisting keeps
- [x] Unit tests: conflict detected + preserved (Manual); fast-forward adopts; each policy resolves
- [x] Land: fold delta into spec/hub.md, append log entry
