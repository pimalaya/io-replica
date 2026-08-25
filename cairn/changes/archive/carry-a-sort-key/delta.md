---
cairn: delta
change: carry-a-sort-key
---

## ADDED Requirements

### Requirement: A placement carries a presentation sort key
`ReplicaPlacement` and `ReplicaFetchedItem` SHALL carry a sort key: the item's
position in its collection's natural order, derived by the consumer wherever the
summary is derived. The engine SHALL treat it exactly as it treats the summary,
as an opaque value it ferries and never parses.

Empty SHALL mean unknown, and SHALL be the default, so an item is orderable from
the moment it exists and no consumer has to invent a value. It is a plain value
rather than an option because that is what the reference storage records, and
one representation of "not known yet" is less to get wrong than two.

### Requirement: A fetch refreshes the key at every tier
An upgrade SHALL adopt the key from the fetched item at both tiers, unlike the
link id, which is kept once resolved. The key is a projection of content rather
than an identity, so the later and better-informed derivation wins: a full body
carries the real date where an envelope may have carried none.

### Requirement: An unknown key never erases a known one
Absorbing an upsert whose key is unknown SHALL leave the shared key alone, on
the same terms as an absent summary. A source that has only probed an item, or
whose kind defines no key, MUST NOT un-sort an item another source has already
placed. A known key SHALL replace another known key.

#### Scenario: A second source probes an item the first summarised
- GIVEN a hub item whose key one source derived
- WHEN another source absorbs the same item with an unknown key
- THEN the projection still carries the derived key

### Requirement: A key survives a rekey
Rebuilding a collection onto a new handle space SHALL carry each placement's key
over, preferring the one the rekey's meta fetch resolved and falling back to the
key the old placement held, so a handle-space change does not un-sort a
collection.

### Requirement: A mutation may restate the key
`Add` SHALL carry a key, and `Edit` SHALL carry an optional one on the same
terms as its optional summary: absent keeps the stored key. An edit that changes
what the key is derived from has to say so, or the item stays where it was in
the list.

## MODIFIED Requirements

None.

## REMOVED Requirements

None.
