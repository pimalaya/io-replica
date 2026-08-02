---
cairn: log
change: fetch-keeps-resolved-link
landed: 2026-08-02
---

# A fetch establishes a link only for a not-yet-linked item

Downstream (neverest over IMAP, a production Posteo account) one message
re-fetched on every sync: its `Meta` tier resolved a link from the server ENVELOPE
(`mid:20230126…@nlnet.nl`, a `Message-ID` the server reports) while its `Full`
tier resolved a *different* link from the parsed body (the body parser surfaced no
`Message-ID` and fell to an `alt:(subject, date, sender)` digest). The upgrade
coroutine overwrote the placement's `link_id` with the fetched item's link
unconditionally (`patched.link_id = Some(item.link_id)`), so the `Full` body fetch
re-identified the already-linked item under the body's link — orphaning the `Meta`
item (never `Full`, re-fetched every sync) and duplicating the message under two
links.

Fix: set `link_id` from the fetch only when the placement has none. An
already-linked placement keeps its link and rises to `Full`; a probed placement
(the `Meta` tier, or a `Full` fetch that skipped `Meta`) still takes the fetched
link. A body fetch no longer re-identifies an item — identity is resolved once.

Two tests added: a `Full` fetch with a divergent link keeps the original link at
level `Full`; a `Meta` fetch of an unlinked item still takes the fetched link. The
full suite (130 unit tests) is green — no test relied on the overwrite. Downstream
live check (neverest → Stalwart, 42 messages incl. 2 with no `Message-ID` using
the `alt:` fallback): all reach `Full`, 0 stuck items, idempotent re-sync.

This supersedes the need to keep the two tiers' link computations byte-identical;
neverest's date-format alignment stays as a display/consistency normalisation.

Spec updated: `sync` (ADDED "A fetch establishes a link only for a not-yet-linked
item").
