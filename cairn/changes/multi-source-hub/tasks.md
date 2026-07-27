---
cairn: tasks
change: multi-source-hub
---

- [ ] Spike: write docs/MULTISOURCE.md; validate the API against cardamum, neverest, himalaya topologies on paper
- [ ] Decide seam: feature-gated `hub` module (recommended) vs core model change
- [ ] Define `OfflineHub` (link id -> shared flags/content/object + per-source bindings/bases)
- [ ] Implement pure `project(hub, source)` and `absorb(hub, source, writes)`
- [ ] Reference test: A edits -> hub -> B derives the push, no bespoke cross-merge
- [ ] Guardrail test: two-source sync of in-agreement items fetches zero bodies
- [ ] Land: fold delta into spec, append log entry
