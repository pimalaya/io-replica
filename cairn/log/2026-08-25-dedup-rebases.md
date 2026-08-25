---
cairn: log
change: dedup-rebases
landed: 2026-08-25
---

# A linked body carries the base with it

Pointing neverest at a real mail account surfaced a change it kept reporting and never made: `update item 101 in Trash`, on every single run. The item was the one message living in two folders.

## What landed

The `Full` upgrade resolves a link id against the object store before fetching, so a message present in two collections is downloaded once. That branch set the placement's body and level and stopped; the fetch branch a few lines below also moves the base onto the body it just stored. A deduped placement therefore held a body its base did not, which is indistinguishable from a staged local edit: the storage projected it dirty, the consumer reported the edit, the next sync derived it again. The branch now rebases.

Alongside it, the same shortcut stopped being taken where it cannot be justified. A link id says two copies are the same item, not that they hold the same bytes; a source that rewrites bodies in place gives each copy its own revision, so linking one copy's body under another's revision records content no fetch ever confirmed. A placement carrying a revision is now left out of the lookup and fetched. Immutable content keeps the dedup, which is what it was written for, and the object store deduplicates the bytes of the fetched body anyway, so the cost is a fetch rather than a body.

## Not repaired

A store already holding such a placement stays dirty: it sits at `Full` with a body, so no upgrade revisits it, and dropping the replica is what clears it. Healing it in the engine would mean reading "body present, base empty" as a mistake, and that is also what a genuine staged edit looks like on a source with no revisions, so the engine would be guessing. Prevention here, repair by resync.

Unlike its neighbour in 0.4.1, where projecting the honest level healed the stored rows for free.

## Capabilities moved

- **sync**: the dedup path now rebases, and only immutable content takes it.
