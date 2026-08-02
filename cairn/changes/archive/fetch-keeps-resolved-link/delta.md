---
cairn: change
change: fetch-keeps-resolved-link
---

## ADDED Requirements

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
