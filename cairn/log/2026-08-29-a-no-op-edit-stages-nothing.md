---
cairn: log
change: a-no-op-edit-stages-nothing
landed: 2026-08-29
---

# An edit restating the synced body stages nothing

`ReplicaMutate` marked every `Edit` dirty without comparing the incoming object against the one the base holds, so an edit re-asserting the synced body minted a placement whose status claimed a pending push while `ReplicaPlacement::staged_edit`, documented as the single reading of that fact, read none. The dirtying is now guarded on the object having changed, and a conflict resolution stays dirty whatever body it carries, the divergence it settles being the change.

The disagreement was visible twice: a consumer rendering status showed unsynced changes for a write that changed nothing, until some later sync scrubbed it, and the sync's edit-beats-delete rule, which asks `staged_edit`, dropped the placement when the source deleted the item while the status said otherwise. No bytes are lost in that shape, the content the placement claimed being the one the source held before deleting it.

The mutable-content property model was wrong on the same fact from the other side: it recorded an edit intent on every successful `mutate`, so it demanded that a body the engine never staged reach the server. It now records one only when the placement holds a staged edit afterwards, which is the reading its own `void_superseded_edits` predicate already used. The delta-debugged five-op sequence is pinned as a regression test.

Spec updated: `mutate` (MODIFIED: the offline mutation vocabulary).
