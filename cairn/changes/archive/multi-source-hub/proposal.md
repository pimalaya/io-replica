---
cairn: change
id: multi-source-hub
status: landed
created: 2026-07-28
---

# Multi-source hub (shared content, per-source base)

## Why
Mirror and symmetric two-way sync need "one logical item, shared content, a base
*per source*" so a change from source A propagates to source B with no bespoke
cross-merge. cardamum-android proves this needs **no merge-core change**: it is a
storage projection (one hub row per link id; `load(collection)` projects a
per-source placement, `write` absorbs back into the hub), and propagation falls
out because B's sync sees the hub content A's sync changed. This change blesses
that pattern into a reusable io-offline layer so neverest / Himalaya / cardamum
stop hand-rolling it.

## What
A design spike first (`docs/MULTISOURCE.md`) settling the seam: an I/O-free,
feature-gated `hub` module of pure functions — `project(hub, source) ->
Vec<OfflinePlacement>` and `absorb(hub, source, writes) -> hub` — that a
consumer's storage wraps. The merge core stays single-source and untouched.

**Hard acceptance criterion (protects partial-cache + on-demand hydration):** the
hub matches items across sources on the *Meta*-level spine (link ids only, no
bodies) and hydrates a body strictly on demand — a two-source sync of items
already in agreement MUST fetch zero bodies.
