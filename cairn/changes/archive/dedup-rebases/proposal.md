---
cairn: change
id: dedup-rebases
status: landed
created: 2026-08-25
---

# A linked body carries the base with it

## Why

`ReplicaUpgrade` resolves a link id against the object store before fetching, so
an item present in two collections costs one download. That branch sets the
placement's body and level and stops there, while the fetch branch a few lines
down also moves the base onto the body it stored. A deduped placement therefore
holds a body its base does not, which is the shape of a staged local edit: a
storage projects it dirty, the consumer reports a change nobody made, and the
next sync derives the same non-change again. It never converges.

It is not a corner case. `lookup_objects` is asked about link ids, not about
collections, so any message living in two folders (an inbox copy and its trashed
one, a thread and its sent copy) takes the branch. It showed on the first live
mail account it was pointed at, on every single run.

The same branch is also making a claim it cannot support where content is
mutable. A link id says two copies are the same item, not that they hold the
same bytes, and a source that rewrites bodies in place gives each copy its own
revision. Linking one copy's body under another copy's revision records content
no fetch ever confirmed, and rebasing it (above) would make that silent instead
of merely dirty.

## What

- The dedup branch rebases the placement's `base.object` onto the linked body,
  exactly as the fetch branch does.
- A placement whose base carries a revision is fetched rather than linked: its
  link id is left out of the lookup, and a hit on it is ignored.

## Scope / non-goals

- A store already holding a deduped-but-unbased placement is not repaired: it
  sits at `Full` with a body, so no upgrade revisits it. Dropping and resyncing
  the replica clears it. Healing it in the engine would mean reading "body
  present, base empty" as a mistake, which is also what a staged local edit
  looks like on a source that has no revisions, so the engine would have to
  guess.
- The object store still deduplicates bytes on write, so a fetched mutable body
  that happens to match another copy is stored once regardless.
