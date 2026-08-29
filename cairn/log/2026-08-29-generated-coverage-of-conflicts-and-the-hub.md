---
cairn: log
change: generated-coverage-of-conflicts-and-the-hub
landed: 2026-08-29
---

# The generators reach the states the properties are named after

Four measured changes to the mutable-content model and one new property file. No production behaviour moves with them; what moves is what the suite generates.

`arb_mut_op` is weighted rather than flat, the two edit variants at four and the upgrade at three against one for everything else, because a content conflict needs a local and a remote edit on the same handle with no sync between them and a thirteen-way flat choice reached that in 4% of cases. The model's resolver now asks for the diverging remote body before resolving, which is what a consumer does and the only thing that ever writes a `conflict_object`. All four models seed five members instead of two, so a delete and a move no longer empty the live set for the rest of the sequence. The crash budget of `a_crashed_write_never_loses_data` is drawn over 0..6 write batches instead of 0..12, the wider range being spent by the terminal drain rather than by the sequence in half the cases.

Measured over 20000 cases before and after:

| state | before | after |
|---|---|---|
| conflict marked | 3.92% | 24.14% |
| conflict marked and resolved | 2.74% | 20.28% |
| `conflict_object` landed | 0.17% | 20.66% |
| same-field collision staged | 10.80% | 32.45% |
| crash fired during the ops | 46.25% | 81.28% |

The higher rates surfaced one failure of the mutable model, at roughly one case in 27000, and it was the model's: an edit restating the body a placement already synced supersedes the edit it overwrites but stages nothing itself, and the ledger recorded a claim for it either way. Pinned as a regression seed and settled with `a-no-op-edit-stages-nothing`. A million generated cases over the whole property file pass after it.

`tests/hub_property.rs` gives the hub axis a model, which it had none of: the state "two sources bound to one item" occurred in exactly zero generated cases at every count, structurally, tests/property.rs holding no hub at all. Three sources over one shared store, each with its own server, driven by generated edits, flag changes, deletes, locally-authored creates, arrivals, server-side deletes and syncs. It asserts four laws: every source bound to an item holds it at the one shared body and the one shared flag set; a source is never read as diverging from itself, which is what `ReplicaSourceBinding::shared_object` was added for; a genuine divergence between two different sources is reported as a conflict keeping both bodies, never silently resolved; and every staged body became the shared one, is held as a reported conflict, or was superseded by a strictly later action on the same item.

The model shadows each source's agreement point from the ops rather than reading it off the binding, so a binding recording the wrong one is a failure rather than an agreement.

It found a real bug unaided, on its first full run and at the default case count, shrinking to a single locally-authored create: see `a-bound-create-is-still-a-create`. 200000 generated cases pass with that fixed.

The hand-written hub fixture (`HubStore`, `SourceStore`) moved to tests/common so both hub targets drive the same store; tests/hub.rs keeps its `Mirror` and its scenarios.

No capability moved: the laws the new file asserts are the ones `hub` already states.
