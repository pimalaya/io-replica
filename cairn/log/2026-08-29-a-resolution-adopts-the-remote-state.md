---
cairn: log
change: a-resolution-adopts-the-remote-state
landed: 2026-08-29
---

# A resolution adopts the remote state it was merged against

A conflict resolved by keeping the body both sides forked from was accepted, reported as resolved, and pushed by nobody. The replica held the ancestor, the remote held its own diverging edit, and the placement rebased clean claiming to be in sync: the user's decision never left the machine and the divergence disappeared from the store, silently, on the one resolution that is both the ordinary three-way merge answer and the one tcard and tcal document by commenting the ancestor line so it stays available.

## What was wrong

The resolution left the base pair contradicting itself. `ReplicaMutation::Edit` took `conflict_revision` into `base.revision` and discarded `conflict_object`, so the base claimed a revision whose content the remote holds a different body for. The sync reads the local side as changed when a dirty placement points at a body its base does not hold, which a resolution to the base body fails by construction, and the remote side as changed when the revision moved, which the resolution had just adopted. Both false is untouched, and the flag pass then rebased the placement to clean.

Nothing was wrong with the sync's comparison, and it is unchanged: what was wrong was the state it was asked to compare.

## What landed

The base a resolution leaves is the remote state it was merged against, both halves of it: the revision observed at conflict time and the body recorded beside it. The four ways a person resolves then fall out of the one comparison, with no special case anywhere. Keeping the local body, keeping the ancestor, or writing a merge of one's own pushes an `Update` gated on the recorded revision; adopting the remote body wholesale owes no push, and the flag pass rebasing it to clean is the right outcome rather than a bug. A remote that moved on since is a fresh divergence and conflicts anew, the resolution surviving as the local side of it.

The base is also the ancestor a later three-way merge runs against, and the same reading holds there: after a resolution, the last state the two sides shared is the remote state the decision was taken against. Merging a later remote edit against the ancestor the user restored would read the restoration as no change at all and silently take the remote.

A conflict holding no base, the create-collision, is given one from the same pair. The spec already claimed "the consumer's resolution re-establishes a base" and nothing did: resolving one left it base-less, so every later sync re-marked it conflicted and its body never pushed. Found while implementing, fixed here because it is the same sentence.

The hub had the same hole on its own axis, and its spec already forbade it: a resolving edit "SHALL also be adopted as the shared body". `reconcile_content` decided whether the source had spoken by comparing the incoming body against the source's sync base, which a resolution keeping the ancestor restates, so the hub kept the body the resolution discarded and every source was handed a decision nobody took. An upsert leaving a conflicted binding now counts as the source having spoken whatever body it carries, which is how the binding's own status already read it.

## Coverage

The mutable-content property model could not reach the shape: its resolver always wrote a body unique in the run, and the four resolutions a real UI offers were three-quarters unexercised. The resolver now keeps the local body, the ancestor, the recorded remote body or a merge of its own, picked from the handle so the strategy keeps its shape and a case replays the same way, and it claims the intent for every resolution whose body the remote does not already hold rather than for every resolution that leaves a staged edit. Reading the claim off the engine's own `staged_edit` is what hid this: the resolution the engine dropped is exactly the one that stages nothing. The model fails on the old code and the delta-debugged case is pinned; the run is green at 100000 cases.

The hub property model does not build per-source conflicted bindings at all, so the hub half is covered by a unit test rather than generated.

## Capabilities moved

- **mutate**: a resolution rebases the base onto the remote state it settled, and gives a base-less conflict a base (new requirement).
- **sync**: the recorded pair is taken into the base on resolution rather than dropped.
- **hub**: an upsert leaving a conflicted binding counts as the source having changed its body.
