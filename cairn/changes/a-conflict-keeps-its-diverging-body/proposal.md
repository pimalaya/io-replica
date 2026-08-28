---
cairn: change
id: a-conflict-keeps-its-diverging-body
status: landed
created: 2026-08-28
---

# A conflict records the revision but not the body it names

## Why

A `Manual` conflict records `conflict_revision`, the remote revision observed when the two sides diverged, and nothing else. The diverging body itself is never stored. Whoever resolves the conflict therefore has to fetch it, which means credentials, a backend and a network round trip in the resolving tool.

That is affordable while the resolver is the sync process. It stops being affordable the moment resolution moves out, which is where consumers are heading: a conflict between two hand-edited cards is a decision for a human, made in an editor or a form, minutes or days after the run that found it, in a program that has no business holding an account password. Everything else that decision needs is already in the store, base body included. The remote side is the one hole, and it turns a pure reader into a full client.

The revision moving is the second half of the problem. An unresolved conflict tracks the newest remote revision on every later sync, deliberately, so the resolving push is gated on what the server actually holds. A body stored once and left alone would silently stop describing the revision recorded beside it, which is worse than not storing it at all: a resolver merging against it would show the user a version nobody holds any more.

The cross-source axis already does this right. A hub conflict keeps the diverging body as `conflict_object` beside the item's own, because a two-way conflict with no common ancestor is unreadable without both sides. The sync axis has the same need and answers it with half the data.

## What

- `conflict_object` on `ReplicaPlacement`, beside `conflict_revision` and carrying the same lifetime: set when the conflict is marked, cleared when it resolves.
- The body arrives the way every other body does, through a fetch the consumer services rather than one the engine performs. Marking a conflict marks the body wanted; the upgrade pass satisfies it, exactly as it revisits a placement whose level claims a tier it does not hold.
- Tracking the revision forward drops the stored body in the same write, so the pair is never half fresh.
- A conflict whose body has not landed yet is visible, listable and unresolvable, which is the same shape as a probed placement whose body has not been pulled.

No change to the hub axis, to the policies, or to what marks a conflict in the first place.
