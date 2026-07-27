---
cairn: change
id: headless-conflict-policy
status: draft
created: 2026-07-28
---

# Headless conflict resolution policy

## Why
Today a content conflict is marked `OfflineStatus::Conflict` and left for a human
to resolve with an edit. That is right for an interactive client (Himalaya), but
a headless, unattended sync (neverest backup / mirror / 2-way over mutable
content — contacts, calendar) has no human at sync time and must resolve by
policy. Immutable-content backends (mail) never content-conflict, so this is a
contacts/calendar concern.

## What
Add `OfflineSyncOptions.conflict: Manual | PreferLocal | PreferRemote | KeepBoth`.
`Manual` is today's behaviour. `PreferRemote` drops the local edit and pulls
(reusing the existing drop-to-Probed, refetch-on-demand path). `PreferLocal`
pushes the local body. `KeepBoth` stages a `Created` duplicate of the local body
under a fresh handle so neither side is lost.
