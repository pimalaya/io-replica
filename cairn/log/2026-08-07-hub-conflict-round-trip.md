---
cairn: log
date: 2026-08-07
change: hub-conflict-round-trip
---

# The hub now round-trips a per-source content conflict

A placement the merge left `Conflict` did not survive `absorb` → `project`:
`absorb_upsert` built every binding as `{ handle, base }`, dropping the status
and `conflict_revision`, and `bound_placement` derived its status purely from
the base comparison, hardcoding `conflict_revision: None`. A storage built on
the hub (io-pimdir, and so neverest) therefore read a conflicted placement back
as `Dirty`.

That broke the merge's own rule that an unresolved conflict is left alone: the
rejected push was re-derived and re-conflicted on every run, with no
convergence, and a consumer could not tell which items needed resolving.
Invisible to immutable-content backends — mail never conflicts — it was found by
neverest's phase-3 fake-remote test before any CardDAV code existed.

`ReplicaSourceBinding` now carries `conflicted` and `conflict_revision`, set by
`absorb` from a `Conflict` upsert and cleared by any other status (so a
resolving edit needs no dedicated call), and projected back ahead of the
Clean/Dirty decision. The state lives on the **binding**, not the item: the
per-source conflict ("this source vs its own remote") is a different fact from
`ReplicaHubItem::conflicted` ("source vs source"), and the two are kept
independent.

Capabilities moved: **hub**. Purely additive to the cross-source path —
`reconcile_content` and `ReplicaHubItem` are untouched.

Breaking: `ReplicaSourceBinding` gained two public fields, so its struct
literals need updating (pre-1.0, minor bump). Persistence is a follow-up in
io-pimdir, which needs a `pimdir` schema change to store them.
