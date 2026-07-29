---
cairn: change
id: mutable-content-across-sources
status: landed
created: 2026-07-28
---

# Mutable content across sources

## Why
The first-cut `hub` adopts the latest source's content as the shared content
(last-writer-wins). For mutable-content backends (WebDAV etags, MS Graph
changeKeys) a genuine both-sides *content* edit across sources then silently
clobbers one edit. The hub must detect the divergence and resolve it by a policy
rather than lose data.

## What
Give `ReplicaHubItem` a `conflicted` flag and a `conflict_object` (the diverging
body), and give `ReplicaHub` a `ReplicaHubConflict` policy (`Manual`,
`PreferIncoming`, `PreferExisting`; default `Manual`). On an upsert, compare the
incoming body against the source's last-synced shared body and the hub's current
shared body: when both moved to different bodies since the source last synced,
that is a conflict. `Manual` flags it and records the diverging body (nothing is
lost, the consumer resolves); `PreferIncoming` is last-writer-wins; `PreferExisting`
keeps the already-shared body. A clean fast-forward (only the source changed)
still adopts the new body. Flags are unaffected (they merge element-wise and
never conflict). Immutable-content backends mint a new link id per body and so
never reach this path.
