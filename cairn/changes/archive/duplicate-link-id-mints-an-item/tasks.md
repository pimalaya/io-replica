---
cairn: tasks
change: duplicate-link-id-mints-an-item
---

# Tasks

- [x] `upgrade.rs`: `write_fetched` mints the losing placement's key from the hint and its handle instead of collecting `(holder, loser)` pairs; `freeze` and `mark_ambiguous` go. The `claimed` map keeps its two sources (the loaded collection and the batch in flight).
- [x] The minted form is `dup:` + hint + `#` + the handle verbatim, with no digest: this crate depends on nothing that hashes, the key is opaque and never parsed, and the handle makes the copy traceable. Document it where it is minted, the three implementations of the format having to agree.
- [x] `sync.rs`: the merge guard deriving nothing for `Ambiguous` goes, `thaw` goes, and the snapshot no longer scans placements for recorded handles.
- [x] `hub.rs`: `is_ambiguous` and the two cross-source exclusions in `project` go; `bound_placement` loses the status branch and `absorb` the round trip.
- [x] `placement.rs`: `ReplicaStatus::Ambiguous` and `ReplicaPlacement::ambiguous_handles` removed; `open.rs` and `mutate.rs` constructors follow.
- [x] `mutate.rs`: the `Ambiguous` refusal (`ReplicaMutateError::Ambiguous`) goes, a mutation against either copy now addressing one item.
- [x] `rekey.rs`: the carry-over of ambiguous handles goes; a minted key is carried like any other; `ReplicaDropReason::Superseded` keeps its licence.
- [x] `storage.rs`: `ReplicaSourceBinding::ambiguous_handles` removed from the seam, which is what lets io-pimdir drop the column.
- [x] Tests: a second copy is minted and stored with its own body; the mint is stable across a fresh store; the mint is decided against the collection and not the batch; both copies propagate to a source holding neither; a rejected push of the duplicate leaves both items intact; a rekey carries a minted key; the previously frozen fixtures are rewritten rather than deleted, since they are the regression this change is judged on.
- [x] `cargo test`, `cargo clippy --all-targets`, `cargo fmt`.
- [x] Release 0.5, breaking: CHANGELOG `### Fixed` for the identity that stored one copy of two and refetched the other every run. No `### Removed` bullet: the ambiguity surface never shipped (it is `[Unreleased]` work on top of 0.4.2), and `[Unreleased]` states the net diff rather than a history of itself, so its `### Added` bullet went instead. The version field is left to the release.
- [x] Fold `delta.md` into `cairn/spec/{upgrade,sync,hub,rekey,storage}.md`; append `cairn/log/YYYY-MM-DD-duplicate-link-id-mints-an-item.md` naming both superseded changes; mark this one `landed` and archive it beside them.
