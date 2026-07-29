---
cairn: log
change: mutable-content-across-sources
landed: 2026-07-28
---

# Mutable content across sources

Replaced the hub's last-writer-wins body adoption with a three-way cross-source
reconcile. `ReplicaHubItem` gained `conflicted` and `conflict_object`, and
`ReplicaHub` a `ReplicaHubConflict` policy (`Manual`, `PreferIncoming`,
`PreferExisting`; default `Manual`). `absorb_upsert` now delegates the body to
`reconcile_content`, which reads the source's last-synced shared body (its
binding base object), the hub's current shared body, and the incoming body: a
clean fast-forward (only the source moved since it agreed) adopts the incoming
body; a divergence (both moved to different bodies) is a conflict resolved by the
policy — `Manual` flags it and records the diverging body losslessly while
keeping the shared one, `PreferIncoming` is last-writer-wins, `PreferExisting`
keeps the shared body. Flags still merge element-wise (never conflict), and
immutable-content backends mint a new link id per body so they never reach this
path.

Four unit tests (clean fast-forward adopts; a divergence conflicts and preserves
both under Manual; PreferIncoming and PreferExisting each resolve). All 123 lib
unit tests plus every integration and property suite pass; fmt, clippy and the
guideline scans clean.

Spec updated: `hub` (ADDED: The hub resolves cross-source content conflicts by
policy). `docs/MULTISOURCE.md` and the module header updated: with delete
propagation and this change, both previously-deferred hub parts are now landed;
what remains is the end-to-end hub-wrapping driver (the neverest rewrite) and the
conflict-resolution round-trip UX.
