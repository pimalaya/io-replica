---
cairn: change
id: flags-unknown-state
status: landed
created: 2026-08-24
---

# An unread flag set is unknown, not empty

## Why

pimdir SPEC §13 gives `items.flags` two distinct absences: `NULL` means the markers have never been read, `'[]'` means the item is known to carry none. The reference storage cannot write the first, because `ReplicaFlags` was a plain set with no state for "nobody has read this yet", so io-pimdir wrote at least `'[]'` for every placement and a probed item claimed to have no markers.

Reading unknown as empty is not a cosmetic loss. An empty set is an opinion: element-wise, it says every flag the other side holds was removed here. A source enumerating without markers (a CardDAV `sync-collection` REPORT returns hrefs and ETags and nothing else) therefore looked like a source that had cleared them, and the merge would carry that absence onto whichever side did know them.

## What

`ReplicaFlags` becomes `Unknown | Known(set)`. The default stays known-empty, so an ordinary write means what it meant before and unknown is a state a probing enumeration states outright rather than one anything falls back into.

The merge treats unknown as no opinion: it neither wins nor loses, the result is whatever the other side reports, and two unknown sides stay unknown. An unknown base is the same fact as no base on the flag axis. The hub gains the rule the sort key already has, so a source that has only probed an item cannot clear what another source read.
