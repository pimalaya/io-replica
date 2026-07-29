---
cairn: tasks
change: cross-source-delete-propagation
---

- [x] Add `deleted` to `ReplicaHubItem`
- [x] `absorb_drop`: mark the item deleted, remove the source's binding
- [x] `absorb_upsert`: clear `deleted` (edit/add beats delete across sources)
- [x] `project`: a deleted item yields a Tombstone for held sources, nothing otherwise
- [x] Unit tests: delete propagates as a tombstone; a deleted item is not copied; a re-add resurrects
- [x] Land: fold delta into spec/hub.md, append log entry
