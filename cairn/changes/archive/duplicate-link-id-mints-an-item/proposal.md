---
cairn: change
id: duplicate-link-id-mints-an-item
status: landed
created: 2026-08-28
---

# A second copy of an identity gets an item, not a freeze

> Cross-repo change, same id in eight repositories, in this order:
> **pimdir** (the rule) → **io-replica** (here: the mint, and the removal of the freeze) → **io-pimdir** (the column goes, the refusal stays) → **io-webdav** (the refusal is named) → **neverest** (the resource name, the push guard, the report) → **himalaya**, **cardamum**, **calendula** (stop assuming a link id is unique).
>
> This **supersedes `duplicate-link-id-freeze`** and **`freeze-is-one-item-wide`** (both landed 2026-08-25). Do this one first in the chain: it is where the state the others persist and report is produced.

## Why

The freeze answered the right question wrongly. Its question stands: two placements resolving to one link id cannot both be linked, because a source binds an identity with one handle, and linking the second overwrites the first binding, destroying the evidence at the write. Its answer was to keep one and record the other, which costs the second copy its existence: no link, no row in the storage, no listing, nothing a user can see or a reader can mirror.

Two findings turned that cost into a defect, both verified 2026-08-28 on a Posteo CalDAV account:

- **A frozen identity stores one of two.** Four `UID`s were held under two hrefs each. Three pairs differed only in `DTSTAMP` and `LAST-MODIFIED`, and the fourth was two genuinely different meetings sharing one `UID`. The replica held four events. The user's fifth, sixth, seventh and eighth existed on the server and in no local row.
- **Stickiness assumed an incremental enumeration.** `upgrade.md` justifies recording the losing handle with "the twin appears in exactly one enumeration, the one that discovers it, and an incremental enumeration never mentions it again". A DAV collection whose server implements no `sync-collection` is listed in full on every run. The twin came back every run, was fetched in full to resolve its identity (no cheap tier for that kind), lost the claim again, and left its body unreferenced: four downloads and four orphan blobs per sync, indefinitely.

The format now gives the second copy somewhere to live: pimdir SPEC §9 makes `link_id` the store's key rather than a restatement of the protocol's identity, so a colliding identity is minted as `dup:<hint>#<handle>` instead of being refused. That turns the engine's whole duplicate apparatus into one branch.

The posture is Postel's, and it is why the propagation rule flips too: liberal in what is read, strict in what is produced. Two resources under one `UID` are two items because that is what the source holds. On the way out nothing is guessed: the item is offered to another source like any other, carrying the `UID` it actually has, and a server refusing it with `no-uid-conflict` produces a rejected push the consumer reports. The engine has no basis for choosing which copy to withhold, and withholding one is how the replica came to hold four of eight.

## What

- **Mint instead of record.** Where `write_fetched` currently pushes a `(holder, loser)` pair into `ambiguous`, it assigns the losing placement a minted link id derived from the hint and its own handle, and links it. The check stays exactly as wide as it is: against the whole collection, through the load by link ids the coroutine already performs, not only against the batch.
- **Delete the freeze.** `ReplicaStatus::Ambiguous`, `ReplicaPlacement::ambiguous_handles`, `ReplicaSourceBinding::ambiguous_handles`, `freeze`, `mark_ambiguous`, `thaw`, the merge guard that derives nothing for an ambiguous placement, the hub's two cross-source exclusions and the rekey carry-over all go. Nothing replaces them: an item with its own key needs no special case anywhere.
- **A minted identity propagates like any other.** It is offered as a `Created` append, it is deleted when its source drops it, and it conflicts and merges on the ordinary rules. A target refusing the duplicate answers with a rejected push, which the engine already models.
- **The mint is deterministic**, from the hint and the handle, so a rebuilt store reproduces the same key and a rekey carries it unchanged (it is a key like any other, and `rekey` already carries keys).

## Scope / non-goals

- **No repair and no survivor.** The engine does not delete a copy, does not merge two, and does not rank them. It stores both and pushes both.
- **No behaviour on the local-authoring path.** A staged `add` colliding with a stored identity still parks (pimdir SPEC §15.3): minting is what reading a source requires, parking is what authoring locally requires.
- **No new storage seam.** The mint is decided from what `ReplicaLoadScope::Links` already returns, so the storage gains nothing and loses a column.
- **The `Meta` tier is unaffected.** Mail still resolves its identity from an `ENVELOPE`, so the mint decision is taken at whatever tier the kind resolves at, and no kind is forced to a body.
