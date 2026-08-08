---
cairn: log
change: carry-a-sort-key
date: 2026-08-08
landed: 2026-08-08
---

# The engine carries a presentation sort key

`ReplicaSortKey` rides beside `ReplicaMeta` on `ReplicaPlacement` and
`ReplicaFetchedItem`, threaded through the hub, the upgrade, the rekey and the
mutations, so a connector's derived ordering reaches storage on the ordinary
write. The engine never parses it, exactly as it never parses the summary.

This was drafted as **deferred** on a cost argument: forty construction sites, a
breaking change to a released crate, to save one `UPDATE` per synced item that
io-pimdir's `set_sort_key` already covers. The call to land it anyway is the
right one for a reason the draft undersold: Pimalaya Android needs date ordering
for its calendar, so the "exactly one consumer wants this" premise was already
expiring, and doing it now means one breaking release rather than two.

## Three decisions the implementation forced

**Empty means unknown, and it is a value rather than an option.** The reference
storage records a `NOT NULL` column defaulting to the empty string, so an
`Option<ReplicaSortKey>` would have created a second way to say "not known yet"
that has to be collapsed at every boundary. One representation is less to get
wrong.

**A fetch refreshes the key at every tier, unlike the link id.** The upgrade
deliberately keeps a link id once resolved, because two tiers disagreeing about
identity strands an item and duplicates it. The key is the opposite: it is a
projection of content, not an identity, so the better-informed derivation should
win. A `Full` body carries the real date where an envelope may have carried
none, and being wrong about it mis-sorts a row rather than losing it.

**An unknown key does not overwrite a known one in the hub.** This is the one
that would have been a silent bug. A second source that has only probed an item
carries no key; adopting it unconditionally would un-sort an item the first
source had already placed, and the un-sorting would show up as a list that
scrambles itself whenever a second replica syncs. The rule mirrors the existing
one for an absent summary. A known key still replaces a known key, so a
correction propagates.

## Rekey

A rebuilt placement prefers the key its meta fetch resolved and falls back to
the one the old placement held. Without the fallback, a UIDVALIDITY bump would
un-sort every item whose meta fetch happened not to resolve, which is precisely
the cached state a rekey exists to preserve.

## Scope

154 tests green, three of them new and all on the hub: a key round-trips through
absorb and project, an unknown key does not erase a known one, and a later
derivation replaces an earlier one. Clippy clean.

Consumers are **not** updated here. io-pimdir binds the field on insert and
update next, and its `set_sort_key` stays as the restating seam for stores
written before this. Neverest and himalaya update on their own schedule; until
they do, they compile against 0.3 and see nothing.

Capabilities moved: **storage** (the key, its tier refresh, its survival through
a rekey), **hub** (the unknown-does-not-erase rule), **mutate** (`Add` and
`Edit` carrying it).
