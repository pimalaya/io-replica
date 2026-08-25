---
cairn: delta
change: flags-unknown-state
---

## ADDED Requirements

### Requirement: An unread flag set is unknown rather than empty
`ReplicaFlags` SHALL carry an `Unknown` state distinct from a known-empty set, since the reference storage records the two apart (pimdir SPEC §13: a `NULL` flags column means never read, `'[]'` means known to carry none). Known-empty SHALL remain the default, so unknown is stated by a source that read no markers rather than fallen back to by an ordinary write.

Only a local placement is ever unknown in practice: a source that reports an item reports what it read. The engine SHALL resolve the state on the first side that carries a set.

### Requirement: An unknown side holds no opinion in the merge
The flag merge SHALL treat an unknown side as neither an addition nor a removal: the result is the other side's set, two unknown sides stay unknown, and an unknown base is the same fact as no base on the flag axis, so nothing is derived from it and both sides' markers are kept.

Reading unknown as empty would make it an opinion, since element-wise an empty set says every flag the other side holds was removed here.

### Requirement: An unknown set never erases a known one
Absorbing an upsert whose flag set is unknown SHALL leave the shared set alone, on the same terms as an absent summary and an unknown sort key. A known set, empty or not, SHALL replace another known set: only unknown is inert.
