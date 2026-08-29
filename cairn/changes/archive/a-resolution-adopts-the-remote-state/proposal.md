---
cairn: change
id: a-resolution-adopts-the-remote-state
status: landed
created: 2026-08-29
---

# A resolution adopts the remote state it was merged against

## Why

A `Manual` conflict holds three bodies: the local one, the base one, and the
remote body at `conflict_revision`. Resolving it with an `Edit` carrying the
**base** body, which is the ordinary "discard both changes, keep the ancestor"
answer every three-way merge UI offers and the one tcard and tcal document by
commenting the ancestor line, was accepted and then pushed by nobody.

The resolution left the base pair contradicting itself: the branch took
`conflict_revision` into `base.revision` and discarded `conflict_object`, so the
base claimed the revision the remote holds a different body at. The next sync
reads the local side as changed when the placement points at a body its base
does not hold, which a resolution to the base body fails by construction, and
the remote side as changed when the revision moved, which the resolution just
adopted. Both false is untouched, the flag pass rebases the placement clean, and
the user's decision never leaves the machine: the replica is silently left
behind the remote and the divergence is gone from the store.

## What

On resolution the base adopts **both** halves of the state the resolution was
merged against, `conflict_object` as well as `conflict_revision`. The base pair
is then consistent again, and the existing positive-signal comparison in the
sync derives the right answer for every way a person resolves, with no change to
the sync at all: keeping the ancestor pushes, keeping the local body pushes, a
custom merge pushes, and adopting the remote body wholesale owes no push and
settles clean.

The base a resolution leaves is also the right ancestor for a later three-way
merge: after resolving, the last state the two sides shared is the remote state
the decision was taken against.

A conflict holding no base (a create-collision) gets one from the same pair,
which is what "the consumer's resolution re-establishes a base" already claimed
and nothing did: resolving one left it base-less, so the next sync re-marked it
conflicted, every run, and the resolution never pushed either.
