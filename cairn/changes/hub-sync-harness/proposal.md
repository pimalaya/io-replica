---
cairn: change
id: hub-sync-harness
status: landed
created: 2026-08-25
---

# The hub, driven by the engine rather than by hand

## Why

`engine-algorithm-audit` called this the largest unknown in the crate, and it was measurable: `grep -rn hub tests/` returned nothing. `ReplicaHub::project` and `absorb` are covered in-crate over hand-built writes, so the loop they exist for had never run once: project a source's view, let a real sync merge and push it, absorb what that sync wrote, project again. Convergence lives in that loop rather than in either half of it.

## What

- `tests/hub.rs`: one shared store behind two sources, each with its own server and its own `ReplicaSyncOptions`, driven through `ReplicaClient`.
- The scenarios the audit named: a member one source holds mirrored to the other, a flag change crossing, a delete crossing, one source refusing removes while the other deletes, a read-only source, and a parity check that one source over the hub reports exactly what the plain store reports.

## Scope / non-goals

- A harness, not a redesign. What it finds is written down; only what it proves wrong is changed.
