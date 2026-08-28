---
cairn: log
change: a-source-cannot-diverge-from-itself
landed: 2026-08-28
---

# A source cannot diverge from itself

The hub decided a cross-source divergence from the base a source last synced with its own remote, and read that base as saying what the source had last agreed with the *hub*. It says nothing of the kind. Only a sync moves it, so a body the source itself folded into the hub and has not pushed yet leaves the same gap another source folding in leaves, and the two were indistinguishable.

A second offline edit was therefore filed as a conflict, in a store with one source bound and no second source anywhere: the hub kept the first body, flagged the item, and recorded the second as the diverging one. The edit that resolves a conflicted binding went the same way, which is the worse half of it: the binding cleared its conflict while the item still held the body the merge replaced, so the next run pushed the unmerged body over the remote the merge was made against.

## What landed

`ReplicaSourceBinding::shared_object` holds the shared body this source last reconciled against, and the cross-source comparison is made against it. Each axis now has its base: `base` is what the source last agreed with its own remote and only a sync moves it, which is what keeps a pending push derivable, and `shared_object` is what it last agreed with the hub, which every live absorb moves to whatever the reconcile settled on, adopted or kept or refused. A refused body still moves it, which is what lets the edit resolving a cross-source conflict fast-forward over the body that won. A `Tombstone` adopts no content and moves nothing.

An upsert carrying the shared body itself now settles nothing either way, where it used to take the fast-forward branch and clear the item's cross-source conflict. That branch was unreachable before, both comparisons being made against the same base, and it becomes reachable the moment they are not: a source merely pushing the shared body to its own server would otherwise have cleared a divergence it had no opinion about.

Nothing else moved. A genuine divergence between two sources conflicts as it did, under every policy, and the flag axis never reached any of this.

## Cost

A persisted field. A storage keeping `ReplicaSourceBinding` keeps this too; io-pimdir gains a nullable column on the binding row, backfilled from the item's own body so a store written before this starts in agreement rather than conflicting once. It names an object and pins none: the value is only ever compared for equality, never read as bytes, and a content-addressed hash compares the same after the body is swept.

## Residual

A source editing a body another source folded in, before its own next absorb, still reads as a divergence: its agreement point is the body before the fold, and nothing in a placement says which body an edit was derived from. `Manual` flags it and keeps both, so nothing is lost.

## Capabilities moved

- **hub**: a binding records what it last agreed with the hub on, the cross-source comparison is made against it, and a resolving edit becomes the shared body.
