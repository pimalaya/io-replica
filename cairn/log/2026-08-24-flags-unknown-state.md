---
cairn: log
date: 2026-08-24
change: flags-unknown-state
---

# An unread flag set is unknown, not empty

`ReplicaFlags` is now `Unknown | Known(set)`, so the state pimdir SPEC §13 records as a `NULL` flags column has somewhere to live. Before it, io-pimdir wrote at least `'[]'` for every placement, and a probed item claimed to carry no markers.

Empty is an opinion: element-wise it says every flag the other side holds was removed here. So a source enumerating without markers looked like a source that had cleared them. The merge now treats unknown as no opinion (the other side's set wins, two unknowns stay unknown, an unknown base is no base on this axis), and the hub gains the rule the sort key already had, so a source that has only probed an item cannot un-flag what another source read.

Known-empty stays the `Default`, so every existing construction means what it meant before; unknown is stated, never fallen into.

Capabilities moved: storage.
