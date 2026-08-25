---
cairn: delta
change: verb-specs
---

## ADDED Requirements

### Requirement: An offline read is a verb
Reading a collection without a remote SHALL go through the same coroutine contract as everything else (`ReplicaOpen`), rather than through a direct storage call. A consumer holding the engine holds one way to reach a replica, so a projection built offline and one built during a sync cannot drift apart, and a storage implements one seam rather than a seam plus a back door.

`ReplicaOpen` SHALL scope its load to the whole collection: it is one of the two verbs that reason about what is *missing* from a replica rather than about named rows, its answer being the projection itself.

### Requirement: A rebuild carries state over by link id
A rekey SHALL match each member of the new handle space to the placement holding the same link id, and carry that placement's body, summary, level, base, flags and pending local state onto the new handle. A member the old space does not account for is an ordinary new placement; a placement the new space does not account for is gone from the source.

Identity is the only thing a handle-space change leaves intact, so it is the only thing the match may key on.

### Requirement: An ambiguity survives a rebuild
A placement carrying ambiguous handles SHALL keep them across a rekey. Renumbering two copies of one identity does not merge them, and a rebuild that cleared the record would resolve the freeze by forgetting why it was frozen.

### Requirement: A rebuild's drops say the row is superseded
Every drop a rebuild emits for a placement its own batch re-writes SHALL carry `ReplicaDropReason::Superseded`, never `Deleted`. The reason is also what licenses the rebind: a storage pins one handle per binding and refuses to repoint it, and a rebuild is the one case where the repoint is correct. The licence is per handle, so a rebuild batch carrying a genuine duplicate SHALL still have that one frozen.

### Requirement: A rebuild is the only bump of the handle-space epoch
The consumer SHALL commit the rebuild's write batch and the collection's epoch bump in one transaction (pimdir SPEC §12). Ordinary syncs, full resyncs from an expired checkpoint and content changes SHALL NOT bump it.

## MODIFIED Requirements

None. The requirements that moved between capability files are unchanged in wording.

## REMOVED Requirements

None.
