---
cairn: log
change: multi-source-hub
landed: 2026-07-28
---

# Multi-source hub

Spiked and shipped the first cut. `docs/MULTISOURCE.md` records the design (the
cardamum-proven shared-hub-with-base-per-source pattern, the map to neverest /
cardamum / himalaya, the zero-bodies guardrail, and the deferred parts). The new
`hub` module (shipped unconditionally — it pulls no deps, so per the guidelines a
feature gate is not justified, a deliberate deviation from the proposal's
"feature-gated") adds `ReplicaSourceId`, `ReplicaSourceBinding`, `ReplicaHubItem`
and `ReplicaHub` with two pure I/O-free functions: `project(collection, source)`
returns the placements a source's `load` should return (each bound item at the
hub's shared content but the source's own base, so a hub change reads as dirty
and the engine pushes it; each missing item whose body is hydrated as a `Created`
append), and `absorb(source, writes)` folds the engine's writes back (adopt the
reconciled content as shared, refresh the source binding, drop a binding on
delete). The merge core is untouched.

Five unit tests, including the hard guardrail (`in_agreement_items_project_clean_
without_a_body`): in-agreement items project `Clean` at their current level with
no object, so a two-source sync fetches zero bodies. Also covered: flag
propagation via absorb→project, hydration-gated append, no-append-without-a-body,
and per-source drop isolation. All 116 lib unit tests plus every integration and
property suite pass; fmt and clippy clean.

Deferred to their own changes (each risks data loss done hastily): cross-source
delete propagation (needs a per-source was-present base), mutable content across
sources (should route through `ReplicaConflictPolicy`), and the end-to-end
storage-that-wraps-the-hub driver (the neverest rewrite).

Spec updated: `hub` (ADDED: The hub composes single-remote sync into
multi-source; Membership propagation is hydration-safe) — a new capability file.
Also fixed a rename leftover in the lib.rs header ("the `Offline` prefix" →
`Replica`) and noted the hub in the header's seams-and-layout section.
