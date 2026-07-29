---
cairn: tasks
change: multi-source-hub
---

- [x] Spike: write docs/MULTISOURCE.md; validate the API against cardamum, neverest, himalaya topologies on paper
- [x] Decide seam: feature-gated `hub` module (recommended) vs core model change
- [x] Define `OfflineHub` (link id -> shared flags/content/object + per-source bindings/bases)
- [x] Implement pure `project(hub, source)` and `absorb(hub, source, writes)`
- [x] Reference test: A edits -> hub -> B derives the push, no bespoke cross-merge
- [x] Guardrail test: two-source sync of in-agreement items fetches zero bodies
- [x] Land: fold delta into spec, append log entry
