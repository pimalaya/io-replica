---
cairn: change
id: body-less-item-is-not-full
status: landed
created: 2026-08-25
---

# A body-less hub item is not `Full`

## Why

A remote content change is pulled by dropping the stale body and lowering the
placement to `Probed` (`pull_content`), so an upgrade refetches it. Through the
hub that repair never happens: `absorb` merges the level as a maximum, the item
keeps the `Full` it reached while it *had* a body, and `ReplicaUpgrade::pending`
skips whatever already reads as `Full`. The item is left claiming a body it no
longer holds, with the summary of the revision before the change, and nothing in
the engine will ever fetch the new one.

The maximum is right for what it was written for: a source that has only probed
an item holds no opinion about its detail, and adopting that opinion would
un-know what another source read. It is the same rule as an unknown flag set or
an absent summary. But a dropped body is not an absence of opinion, it is a
fact, and the two got conflated because one field was made to carry both.

Only mutable content reaches it. Mail bodies are immutable and carry no
revision, so nothing ever refreshes; the first consumer to hit it was neverest's
CardDAV side, where a card edited on the server stayed stale for good and was
re-downloaded on every run without the write ever landing.

## What

- `ReplicaHubItem::stored_level`: `Full` requires a stored body, so an item
  holding none reads one rung down (its cached summary is still there, the body
  is not).
- `absorb` records that level rather than the raw maximum, so the storage under
  the hub stops persisting the false claim.
- `project` reports it too, so a store **already written** in that state heals:
  an upgrade reads the projection, sees the item is not `Full` and refetches.

## Scope / non-goals

- The level stays the high-water mark across sources for everything else; only
  the body's absence overrides it.
- `pull_content` is untouched: lowering to `Probed` and dropping the base object
  is already what a refresh should do.
