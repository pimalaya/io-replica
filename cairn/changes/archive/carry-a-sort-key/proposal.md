---
cairn: change
id: carry-a-sort-key
status: landed
created: 2026-08-08
---

# Carry a presentation sort key alongside the summary

## Why

pimdir gained `items.sort_key` (SPEC §9.3): one TEXT column per item giving its
position in its collection's natural order, so a reader can ask for the newest
fifty messages, this week's events, or contacts from A. Without it the only
orderings a store can serve are by link id or by allocation order, neither of
which means anything to a reader, and every consumer has to scan a whole
collection into memory to render a list.

The key is derived where the summary is derived: a connector parsing a body or
an envelope already has the date, the display name, the start time. But there is
nowhere for it to put one. `ReplicaFetchedItem` carries `meta` and nothing
beside it, and `ReplicaPlacement` likewise, so the key cannot ride the ordinary
write into the store. The engine would only ever ferry it, exactly as it ferries
`meta`, which it never parses either.

## What

Add a sort key beside `meta` on `ReplicaFetchedItem` and `ReplicaPlacement`, and
thread it through the merge the way `meta` is already threaded, so a
`ReplicaWriteOp::UpsertPlacement` carries it into storage.

Empty means unknown, matching the store, so an item is orderable from the moment
it exists and no consumer is forced to invent a value.

## Why this is a draft and not done

The blast radius is out of proportion to the gain **today**, and the honest
thing is to say so rather than land it because it was on a list.

`meta` is constructed at around forty sites across `hub`, `sync`, `mutate`,
`upgrade` and `rekey`, most of them test literals. A parallel field means
touching all of them, and it is a breaking change to a released crate, so
neverest and himalaya update with it. What it buys is saving one `UPDATE` per
newly synced item, because io-pimdir already exposes `set_sort_key` for exactly
this gap: a consumer owns its meta convention, so it can derive keys from
summaries it wrote itself and restate them after a sync.

The restating pass has one real weakness worth recording, so this is not
dismissed on cost alone: between the sync's write and the restate there is a
window where an item has a summary and no key, and a crash inside it leaves the
item unordered until the next pass. That is recoverable and idempotent, where
the field would be atomic. It is not enough to justify the churn while exactly
one consumer wants the key.

Revisit when a second consumer needs it, or when this crate next takes a
breaking change for another reason and the cost is already being paid.
