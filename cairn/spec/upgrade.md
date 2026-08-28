---
cairn: spec
capability: upgrade
status: current
---

# Upgrade

`ReplicaUpgrade` is the I/O-free coroutine that raises placements up the detail
ladder: `Probed` (the handle exists), `Meta` (its summary and identity are
known), `Full` (its body is stored). Enumeration stays cheap because it stops at
the first rung, and hydration is a separate verb the consumer runs when it wants
the payload, for the members it wants it for.

Two things make it more than a fetch loop. It **resolves identity**, since a
probed placement has no link id until something reads one, and identity is what
every other verb keys on; and it **avoids fetching what the store already
holds**, since one body may serve several collections.

What the merge does with a hydrated placement lives under [sync](sync.md); what
a rebuilt handle space does with one lives under [rekey](rekey.md).

### Requirement: Fetch batches are order-independent
A `WantsFetch` batch SHALL impose no ordering on its handles: the consumer MAY
fetch them in any order and concurrently, and SHALL return results keyed by
handle. The engine SHALL match fetched results by handle, not by position, so a
consumer servicing the batch across a connection pool is correct.

### Requirement: A linked body carries the base with it
A `Full` upgrade that resolves a placement's body from the object store instead
of fetching it SHALL record that body as the placement's base content, exactly
as the fetch path does. A placement holding a body its base does not is the
shape of a staged local edit, so a storage projects it dirty and the consumer
re-derives a change nobody made, on every sync, without ever converging.

### Requirement: A mutable body is fetched, never linked
A placement whose base carries a revision SHALL be fetched rather than linked
from the object store: its link id is left out of the lookup, and a hit on it is
ignored. A link id says two copies are the same item, not that they hold the
same bytes, and a source that rewrites bodies in place gives each copy its own
revision, so linking one copy's body under another's would record content no
fetch confirmed. Immutable content keeps the dedup, which is what it is for, and
the object store deduplicates the bytes of a fetched body regardless.

#### Scenario: The same message in two collections downloads once and reads clean
- GIVEN a based placement with no revision whose link id the object store already holds a body for
- WHEN it is upgraded to `Full`
- THEN it is written at `Full` with that body, its base holds the same body, and no fetch is requested

#### Scenario: A revision-carrying placement is fetched
- GIVEN a based placement carrying a revision whose link id the object store already holds a body for
- WHEN it is upgraded to `Full`
- THEN the body is fetched from the remote rather than linked from the store

### Requirement: A fetch establishes a link only for a not-yet-linked item
Applying a fetched item SHALL set the placement's `link_id` from the fetch **only
when the placement has none** — a `Meta` upgrade of a probed placement, or a
`Full` fetch of an item that never resolved a link. An already-linked placement
SHALL keep its link id when a later fetch (in particular a `Full` body fetch)
returns a different one, and simply rise to the fetched tier. A body fetch does
not re-identify an item; identity is resolved once, at the first fetch that
carries a link. This prevents a two-tier link disagreement (a server ENVELOPE
`Message-ID` the body parser misses, or a differently formatted fallback-digest
date) from stranding the linked item and duplicating it under the body's link.

#### Scenario: Largest-first hydration overlaps a heavy message
- GIVEN a consumer that fetches a Full batch across a bounded connection pool, largest first (by its own member sizes)
- WHEN the batch is hydrated
- THEN the heavy member is fetched concurrently with the light ones, results are matched by handle, and no fetch order is assumed

### Requirement: An upgrade revisits what it never got
An upgrade SHALL revisit a placement whose level claims a tier it does not hold: `Full` with no object, `Meta` with no summary. The level is a claim and the payload is the fact, and nothing else revisits what already reads as reached, so such a row would be skipped for good.

#### Scenario: A body-less full row
- GIVEN a placement recorded at `Full` holding no object
- WHEN it is upgraded to `Full`
- THEN it is fetched rather than skipped

### Requirement: A fetch never establishes a link the collection already holds
Applying a fetched item SHALL NOT set a placement's `link_id` to one another placement of the same collection already carries, whether that other placement is in the same batch or only in the store. The engine identifies a placement by its collection and link id, and a source binds it with one handle, so a second placement resolving to the same identity cannot take the key: taking it would overwrite the first binding's handle, and the fact that the source holds two resources would be lost at that write, before any later rule could act on it.

The second placement SHALL instead be linked under a minted key, per the requirement below. The check SHALL be made against the whole collection, not only the placements being upgraded, since a batch hydrating just the second copy would otherwise link it under the key the first already holds.

#### Scenario: A second copy is minted, not linked to the same key
- GIVEN a collection whose placement already holds a link id
- WHEN a fetch resolves another handle of that collection to the same link id
- THEN the first placement keeps its key and its handle, and the second is linked under a minted one

### Requirement: A second copy of an identity is minted, not withheld
A fetch resolving a placement to a link id another placement of the same collection already carries SHALL give that placement a **minted** link id (pimdir SPEC §9, `dup:<hint>#<handle>`) and link it, rather than leaving it unlinked. The minted form SHALL be derived from the hint and the placement's own handle alone, so it is deterministic: the same collection re-read from scratch mints the same key, and a rebuild carries it rather than re-deriving it.

The engine identifies a placement by its collection and link id, and a source binds one identity with one handle, so two placements cannot share one key. What follows from that is which of the two gets the key, not that one of them must go without: a source holding two resources is holding two items, and an engine that stores one of them is losing data at the point where it noticed the problem.

Minting SHALL be decided against the whole collection rather than the batch, through the load by link ids the upgrade already performs, since a batch hydrating only the second copy would otherwise take the key. Which copy keeps the bare hint SHALL follow from the handles rather than from the order the fetch replied in, a batch being order-independent: a mint that depended on which copy a connection pool finished first would not survive a rebuild.

#### Scenario: The second copy is stored
- GIVEN a collection whose placement already holds a link id
- WHEN a fetch resolves another handle of that collection to the same link id
- THEN that placement is linked under a minted key, with its own body, meta and base

#### Scenario: The mint is stable
- GIVEN a collection whose duplicate was minted
- WHEN the same collection is enumerated and hydrated again from an empty store
- THEN the same handle receives the same minted key

