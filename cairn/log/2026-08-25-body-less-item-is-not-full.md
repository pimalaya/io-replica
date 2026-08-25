---
cairn: log
change: body-less-item-is-not-full
landed: 2026-08-25
---

# A body-less hub item is not `Full`

A remote content change never reached a hub-backed store. `pull_content` does the right thing (drop the stale body, lower the placement so an upgrade refetches), but `absorb` merged the level as a maximum, so the item kept the `Full` it had reached while it still held a body, and `ReplicaUpgrade::pending` skips whatever already reads as `Full`. The result: an item claiming a body it does not have, carrying the summary of the revision before the change, with no fetch ever derived for it.

## What landed

`ReplicaHubItem::stored_level` states the invariant the level and the object were quietly disagreeing about: `Full` means a stored body, so an item without one reads one rung down. `absorb` records it after the content merge, and both projections (`bound_placement`, `tombstone_placement`) report it.

Recording *and* projecting is deliberate. Recording keeps the storage under the hub from persisting a false claim; projecting is what heals a store already written in that state, since an upgrade reads what `load` projects rather than the stored row. A store stuck this way needs no migration and no resync: the next run projects `Meta`, the upgrade refetches, and the fetch rebases (`base.object` and `base.revision` travel with the body), so the run after it is quiescent.

The maximum stays for everything else, and its reason is untouched: a source that has only probed an item holds no opinion about its detail, exactly as an unknown flag set or an absent summary holds none, and adopting that opinion would un-know what another source read. What the fix separates is the one case that is not an absence of opinion but a fact: the body is gone.

## How it was found

Only mutable content reaches it, mail bodies being immutable and revision-less, so the fake remotes in this crate's own tests never produced a refresh through a hub. It surfaced running neverest's CardDAV end-to-end test against a live Radicale: a card edited on the server stayed stale for good and was re-downloaded on every run without the write ever landing. That test now passes end to end, and a store left stuck by the old behaviour was watched healing on its next sync.

## Capabilities moved

- **hub**: `Full` now means a stored body, in what `absorb` records and in what `project` reports.
