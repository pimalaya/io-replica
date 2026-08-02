---
cairn: change
id: fetch-keeps-resolved-link
status: landed
created: 2026-08-02
---

# A fetch establishes a link only for a not-yet-linked item

## Why

The upgrade coroutine overwrote a placement's `link_id` with every fetched
item's `link_id`, unconditionally — including a `Full` body fetch of an item the
`Meta` tier had already linked. When the two tiers disagree on the link, this
strands the item: a downstream consumer (neverest over IMAP) resolves the link
at `Meta` from the server ENVELOPE and again at `Full` from the parsed body, and
these can differ — a server reporting a `Message-ID` in the ENVELOPE that the body
parser does not surface (falling to a `(subject, date, sender)` digest), or a
differently formatted date in that digest. The `Full` fetch then re-identified the
already-linked item under the body's link, leaving the `Meta` item orphaned
(never `Full`, re-fetched every sync) and duplicating the message under two links.
A body fetch should never re-identify an item; identity is resolved once.

## What

In the upgrade's fetch-apply, set the placement's `link_id` from the fetched item
**only when the placement has none** (a `Meta` upgrade of a probed placement, or a
`Full` fetch of an item that skipped `Meta`). An already-linked placement keeps
its link and simply rises to `Full`. Matching stays by handle. Two tests pin it: a
`Full` fetch with a divergent link keeps the original link and reaches `Full`; a
`Meta` fetch of an unlinked item still takes the fetched link.
