---
cairn: log
date: 2026-08-25
change: merge-join
---

# The merge joins two sorted sides instead of copying them

The merge used to start by building a `BTreeSet` union of the local and remote key spaces, cloning every handle into it, then cloning every remote item out of its map, then cloning the whole placement per candidate. Three copies of the collection to produce a walk over two sides that were both ordered already.

They are joined now. `Join` walks a `BTreeMap::into_iter` beside the sorted snapshot with two peekables, yielding a `Candidate` per handle: the placement, the item, or both. It owns both sides, which is the part that matters, because owning them is what lets the merge *take* a placement rather than copy one. The coroutine hands its local map over with a `mem::take` (nothing else reads it after the load), and the four arms of the merge that used to work through `local.as_ref()` now own what they were handed, so the read-only revert writes the placement it was given, and the resurrect and create paths move theirs into the pending map instead of cloning it beside the change. What remains is one copy per candidate that actually writes, which is the one a write takes.

## Measured

On 100k members, release build, against the in-memory reference storage:

- a cold full sync (every member new upstream, so a write each): **118 ms**;
- a warm full sync (both sides holding every member, a flag pull each): **225 ms**;
- the union-and-clone pass alone, over the same 100k on both sides: **83 ms**.

That last figure is the work removed, measured on its own: it is what the old shape spent before the merge looked at a single placement, against a 225 ms merge that now does the whole job. Its cost is gone entirely rather than reduced, since nothing replaced it: the join is the walk the merge was going to do anyway.

## The write batch is bounded too

A merge over a whole collection held one write per member until the last candidate was resolved. It now hands a batch over at `ReplicaSync::WRITE_CHUNK` (1024) writes and picks up where it left off, which is why the merge is now a resumable state (`Merge`, held across yields) rather than a single pass inside one `resume`.

The bound is memory, not the crash window that `chunked-pushes` bounds: a lost write batch costs a re-merge, which is free, where a lost push costs a round trip. That is why it is 1024 rather than 64.

**The cut is between candidates and never inside one**, and this is load-bearing rather than tidy. A keep-both resolution writes the pulled placement *and* the local body staged beside it as a new member; a boundary between those two writes would lose that body if the batch after it never landed. The check therefore sits in the candidate loop, not in whatever pushes a write, and a test pins it by arranging for the boundary to fall exactly on such a resolution.

The checkpoint rule from `chunked-pushes` is what makes a partially merged run safe: an intermediate batch carries no checkpoint, so an interrupted merge re-enumerates from the same cursor and re-derives what it had not recorded. What is new is that a partially merged run now leaves its prefix applied where it used to leave nothing; each written row is individually reconciled, so the replica is consistent either way, and a delta re-lists everything since the unmoved cursor.

## What the tests caught

The property suite earned its keep immediately. The first cut of the delta rule dropped a candidate whose placement had no base, because it read the missing base as "nothing to synthesize a remote state from" and returned nothing. That handle is precisely a staged create: never based, never mentioned by an enumeration until it lands, and the old code kept it as a candidate with no remote item. `mutable_interleavings_converge_after_resolution` and `a_crashed_write_never_loses_data` both shrank to the same minimal case, a copy into the archive left pending forever.

Four tests were added beside it: an unordered enumeration derives exactly what the sorted one derives (pushes, writes and report compared whole); a handle listed twice is merged once rather than pulling a phantom member; a merge larger than one batch hands over several, with the checkpoint only in the last; and the keep-both boundary case above. The chunked-pushes test was folded onto the same driver, which services any number of pushes and writes.

## Not done

The derived pushes are still accumulated whole before the first chunk goes out, so a run holds one `ReplicaChange` per change it derived. Bounding that means deriving and pushing interleaved, which is a different change: the pending maps are keyed by handle and would carry across chunk boundaries.

Capabilities moved: `sync`.
