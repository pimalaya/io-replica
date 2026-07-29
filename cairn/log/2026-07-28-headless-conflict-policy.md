---
cairn: log
change: headless-conflict-policy
landed: 2026-07-28
---

# Headless conflict resolution policy

Added `ReplicaConflictPolicy` (`Manual` / `PreferLocal` / `PreferRemote` /
`KeepBoth`, default `Manual`) and a `conflict` field on `ReplicaSyncOptions`. The
both-sides-edited branch of `reconcile_content` now dispatches to a new
`resolve_conflict`: `Manual` marks the conflict (the prior behaviour, extracted
into `mark_conflict`); `PreferRemote` pulls the remote and drops the local edit;
`PreferLocal` pushes the local body as an `Update` gated on the *observed* remote
revision so it overwrites rather than being rejected, and falls back to `Manual`
when content pushes are forbidden; `KeepBoth` pulls the remote into the placement
and stages the local body as a fresh `Created` member (via `stage_conflict_dup`,
keyed under a `\u{1}keepboth` handle) for the next sync to append. The base-less
create-collision path is untouched (always a conflict), and immutable-content
backends never reach a content conflict, so mail is unaffected.

Four unit tests added (PreferRemote drops the edit; PreferLocal overwrites with
the observed revision; PreferLocal falls back to conflict with no content right;
KeepBoth pulls and stages the duplicate). All 111 sync unit tests plus
integration and property suites pass; fmt and clippy clean. Consumers construct
`ReplicaSyncOptions` with `..Default::default()`, so the new field is source-
compatible.

Spec updated: `sync` (ADDED: Headless conflict resolution).
