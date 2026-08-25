---
cairn: change
id: merge-join
status: active
created: 2026-08-25
---

# The merge joins two sorted sides instead of copying them

## Why

`full_candidates` builds a `BTreeSet` union of the local and remote key spaces, cloning every handle, then clones every `ReplicaRemoteItem` out of the remote map. `merge` then clones the whole `ReplicaPlacement` per candidate, and `pull_flags`, `rebase`, `pull_content` and `mark_conflict` each clone it again before pushing it into the write batch.

Both sides are already ordered: the local state is a `BTreeMap`, and a consumer can deliver its snapshot sorted. So the union is a two-pointer merge-join written as a set build, and a 100k-item full sync allocates on the order of 300k placement and handle clones to produce a result it could have walked. The whole local map and an unbounded write vector are held at once.

Nothing here is wrong; it is a shape that costs memory and time proportional to the collection on exactly the path that runs over the whole collection.

## What

- Merge-join `local` against a sorted remote slice, walking both in order rather than materialising their union.
- Operate on `&ReplicaPlacement` and own at most one copy per handle, the one a write actually takes.
- Chunk the write batch alongside the pushes (`chunked-pushes`), so neither side is unbounded.

## Scope / non-goals

- No behaviour change. The candidate set, the order it is merged in and every derived change stay identical; the property suite comparing a delta against a full sync is the guard.
- The delta path keeps its own candidate rule (changed, vanished, and locally non-clean handles); only the complete-snapshot path becomes a join.
- Requires the remote snapshot to be sorted by handle. State it, and sort defensively if it is not.
