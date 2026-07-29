---
cairn: log
change: cross-source-delete-propagation
landed: 2026-07-28
---

# Cross-source delete propagation in the hub

Gave `ReplicaHubItem` a `deleted` flag. `absorb`'s drop path now marks the item
deleted and removes the dropping source's binding (a bound member removed is a
genuine delete). `project` dispatches on `(deleted, bound)`: a deleted item still
held by the source yields a `Tombstone` (content kept, so the engine's
edit-beats-delete rule can still resurrect it) so its next sync pushes a
`Remove`, and yields nothing for a source that lacks it (a deleted item is never
re-copied). Once no source holds the item it is pruned. A live upsert clears
`deleted`, so a re-add or an edit-beats-delete resurrection brings the item back
everywhere.

Three unit tests added (a delete projects a tombstone on the other source and
nothing on the deleting one; a deleted item is pruned once every source
propagates; a live upsert resurrects a delete in flight and re-copies it). All
119 lib unit tests plus every integration and property suite pass; fmt, clippy
and the guideline scans clean.

Spec updated: `hub` (ADDED: The hub propagates a delete across sources). One
deferred hub item remains: mutable content across sources.
