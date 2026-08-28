---
cairn: log
change: duplicate-link-id-mints-an-item
date: 2026-08-28
---

# A second copy of an identity gets an item, not a freeze

This **supersedes `duplicate-link-id-freeze`** and **`freeze-is-one-item-wide`**, both landed 2026-08-25: same defect, opposite answer. The first recorded the losing handle on the placement that held the identity and derived nothing for it; the second stated the scope of that freeze and pinned it with a test. Both are reversed here, and the whole apparatus is deleted rather than narrowed.

The question they answered stands: two placements resolving to one link id cannot both be linked, because a source binds an identity with one handle and linking the second overwrites the first binding, destroying the evidence at the write. What was wrong was the answer. Keeping one copy and recording the other costs the second copy its existence: no link, no row, no body, no listing, nothing a user can see or a reader can mirror.

Two findings, verified 2026-08-28 against a Posteo CalDAV account, turned that cost into a defect:

- **A frozen identity stored one of two.** A calendar of 454 items held four `UID`s under two hrefs each, one named `<uid>@google.com.ics` and one `<uid>%40google.com.ics`, both written by Thunderbird. Three pairs differed only in `DTSTAMP` and `LAST-MODIFIED`; the fourth was two genuinely different meetings sharing one `UID`. The replica held four events. The user's fifth, sixth, seventh and eighth existed on the server and in no local row.
- **Stickiness assumed an incremental enumeration.** The freeze was justified by "the twin appears in exactly one enumeration, and an incremental one never mentions it again". That server offers no `sync-collection`, so the collection is listed in full on every run: the twin came back every run, was fetched in full to resolve its identity (a calendar object has no cheap tier), lost the claim again, and left its body unreferenced. Four downloads and four orphan blobs per sync, indefinitely.

## What landed

- **The mint** (capability `upgrade`). `ReplicaUpgrade::write_fetched` gives the losing placement a link id of its own instead of collecting `(holder, loser)` pairs: `dup:`, the identity hint the fetch resolved, `#`, and the placement's handle verbatim. No digest, this crate depending on `log` and `thiserror` and nothing that hashes; the key is opaque, never parsed back, and carrying the handle makes the copy traceable to the resource it came from. The form is fixed by pimdir SPEC §9, three implementations having to agree on it, so it is spelled out where it is minted.

  The claim check keeps both its sources, the loaded collection and the batch in flight, and keeps its width: a batch hydrating only the second copy still asks the one scoped `ReplicaLoadScope::Links` read before writing, since it would otherwise take the key the holder already has. The mint happens only where a placement has no link id, so a handle re-fetched keeps the key it was given and is never minted twice.

  One thing the proposal did not spell out and the tests forced: a fetch batch is order-independent by contract, so which copy keeps the bare hint may not follow from which one a connection pool finished first. The batch is claimed in handle order, which is what makes a store rebuilt from scratch reproduce the same keys.

- **The freeze is gone** (capabilities `sync`, `hub`, `rekey`, `storage`). `ReplicaStatus::Ambiguous`, `ReplicaPlacement::ambiguous_handles`, `ReplicaSourceBinding::ambiguous_handles`, `freeze`, `mark_ambiguous`, `thaw`, the merge guard that derived nothing, `ReplicaMutateError::Ambiguous` and its refusal, the hub's `is_ambiguous` with both cross-source exclusions, the status branch in `bound_placement`, the round trip through `absorb`, and the rekey carry-over. Nothing replaces any of them: an item with its own key needs no special case anywhere, which is the whole point of minting. Dropping the binding field is what lets io-pimdir drop its column next.

- **A minted identity is an ordinary item** (capability `hub`). It is offered to a source that lacks it as a `Created` append, its drop marks the shared item deleted, and it merges and conflicts on the ordinary rules. A target that refuses the duplicate says so itself, with a protocol-level `no-uid-conflict`, and that refusal is a rejected push the consumer reports. Liberal in what is read, strict in what is produced: the engine has no basis for choosing which copy a user may have on the other side, and withholding one is how the replica came to hold four of eight.

## Not in scope, as proposed

No repair and no survivor: the engine does not delete a copy, does not merge two and does not rank them. No re-canonicalisation either, so a minted key stays minted after its bare twin is deleted, an opaque key being one a consumer has already shown. The local-authoring path is untouched: a staged `add` colliding with a stored identity still parks (pimdir SPEC §15.3), minting being what reading a source requires and parking what authoring locally requires.

`rekey` carries a minted key like any other but mints none of its own. A rebuild re-resolves identities from the new spine, where both copies report the bare hint again, so a renumbered collection that still holds a duplicate lands two placements on the hint until the next fetch path settles it. That hole predates this change and is unchanged by it; closing it belongs to a rekey-side claim check, not here.

## Verification

- 220 tests green (184 lib, 36 integration), `cargo clippy --all-targets` clean, `cargo fmt`.
- `tests/duplicate_link_id.rs` is rewritten to the new outcome rather than deleted, being the regression this change is judged on: the second copy is minted and stored with its own body; the mint is stable across a fresh store hydrated in the other order; the second copy is fetched once and kept, which is the Posteo finding directly; a vanish removes the copy that went and no other; each copy reconciles on its own, one pulling a remote flag while the other pushes a staged one.
- `upgrade` unit tests cover the batch case, the collection-not-batch case and the no-double-mint case; `tests/hub.rs` covers both copies reaching a source that holds neither, and a refused duplicate leaving both items intact through a remote that answers `no-uid-conflict`; `rekey` covers a carried minted key with its pending push.

Capabilities moved: `upgrade`, `sync`, `hub`, `rekey`, `storage`.
