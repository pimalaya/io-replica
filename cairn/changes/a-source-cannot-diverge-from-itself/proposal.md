---
cairn: change
id: a-source-cannot-diverge-from-itself
status: landed
created: 2026-08-28
---

# A source reads its own unpushed edit as another source disagreeing

## Why

The hub decides a cross-source divergence from three bodies: the source's last-synced base, the hub's current shared body, and the incoming one. Both sides having moved to different bodies is a conflict, and `hub_moved` is meant to say "another source folded a body in since this source last agreed with the shared one".

It does not say that. A binding's base answers to the source's own remote and only advances when a sync agrees with it, so a body this very source folded into the hub and has not pushed yet also leaves the base behind the shared one. The gap the test reads is the same gap in both cases, and the engine cannot tell them apart.

With one source and no second source anywhere, a first offline edit is adopted, and a second one arriving before the first is pushed reads as two sources disagreeing: under `Manual` the hub keeps the first body, flags the item conflicted and files the second as the diverging one. The edit is dropped, in a store that has a single source. The same shape drops a resolving edit on a conflicted binding, which is worse than a lost edit: the merged body never becomes the item's body, so the next run pushes the unmerged one over the remote the merge was made against.

Consumers have started papering over it. Before draining a resolving edit, neverest rewrites the binding's base object to the placement's current body so the comparison reads false, which is a caller reaching into a base that belongs to the sync axis to correct an answer on the hub axis.

## What

The binding records the shared body this source last reconciled against, and the cross-source comparison is made against that rather than against the sync base.

The two axes then have a base each. `base` is the state last agreed with the source's own remote and only a sync moves it, which is what conditions a push. `shared_object` is the state last agreed with the hub and every absorb of that source moves it, which is what a cross-source merge needs. Reading one as the other is the bug, and no single field can serve both: the push must stay derivable while the source is ahead of its remote, which is exactly the window in which the source is not behind the hub.

The alternative, recording on the item which source last set the shared body, is a cheaper column and not the same fact. It answers for the last writer only, so a source that has already folded a body in but did not write it last still reads as behind: a two-spoke store resolving a conflict on the spoke that received the body rather than the one that authored it drops the merge exactly as today. Per-source is what the question is.

An upsert adopting no content moves no agreement point, so a `Tombstone` keeps whatever the binding held.

The cost is a persisted field. io-pimdir stores `ReplicaSourceBinding` and gains a column for it, nullable, folded into schema version 1 with the reconcile-on-open path, backfilled from the item's own body so a store written before this lands starts in agreement instead of conflicting once. It names an object and does not pin one: the value is only ever compared for equality, never read as bytes, and a content-addressed hash compares the same after the body is swept. No refcount, no reachability question.

Until a source has folded once the field is empty, and the sync base stands in for it, which is exactly today's answer for a binding this hub has never absorbed.

## Residual

A source editing a body another source folded in, before its own next absorb, still reads as a divergence: its agreement point is the body before the fold, and the hub cannot know whether the edit was made on top of what it projected or on top of what the source last agreed with. `Manual` flags it and keeps both bodies, so nothing is lost, and answering it would take recording the body an edit was derived from on the placement itself.
