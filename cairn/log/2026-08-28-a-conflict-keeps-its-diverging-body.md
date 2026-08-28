---
cairn: log
change: a-conflict-keeps-its-diverging-body
landed: 2026-08-28
---

# A conflict keeps the body it diverged from

A `Manual` conflict recorded `conflict_revision`, the remote revision observed when the two sides diverged, and nothing else. Whoever resolved it had to fetch the body that revision named, which meant credentials, a backend and a round trip in the resolving tool. Affordable while the resolver is the sync process, and not once resolution moves out: a conflict between two hand-edited cards is a decision for a human, made in an editor minutes or days later, in a program with no business holding an account password.

## What landed

`ReplicaPlacement::conflict_object` holds the remote body at the recorded revision, with the same lifetime as the revision itself: set when the conflict is marked, cleared when an edit resolves it, and dropped in the same write that advances the revision. Half a pair is worse than none here, since a body outliving the revision beside it describes a version the server no longer holds, and a resolver merging against it would show the user a phantom.

The engine still fetches nothing. Marking a conflict marks the body *wanted*, which is what a conflicted placement holding no conflict object means, and the upgrade pass supplies it the way it supplies any other body. That pass now revisits such a placement, which the level rule alone would skip: it reads as `Full` and holds a body, just not the one it is missing. What it fetches lands on `conflict_object` and nowhere else, the placement's own object being the local side of the divergence and the fetch answering the other question entirely. A conflict whose body has not landed yet is visible and listable and not resolvable, the same shape as a probed placement with no body.

`ReplicaSourceBinding` round-trips the new field beside `conflicted` and `conflict_revision`, since io-pimdir persists the binding and that is the type it persists. The item-level `ReplicaHubItem::conflict_object`, the cross-source axis, is untouched and stays independent: one says a source and its own server disagree, the other that two sources do.

A rekey carries the stored body only while the revision it was fetched at survives the renumbering, and drops it otherwise, which is the same rule the sync applies.

## Not changed

No policy moved. `PreferLocal`, `PreferRemote` and `KeepBoth` settle within the run and record neither half of the pair, and what marks a conflict in the first place is what it was. Immutable-content backends report no revision, so they never reach any of this.

## Capabilities moved

- **sync**: a marked conflict records the diverging body beside the revision, and drops it when the revision moves.
- **upgrade**: a conflicted placement is revisited for the body it lacks, and the fetch lands on the conflict object rather than on its own.
