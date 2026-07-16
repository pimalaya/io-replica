# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-16

### Added

- Added the five I/O-free coroutines (open, upgrade, mutate, sync, rekey): no_std state machines over the two-axis model of content-addressed Objects and per-collection Placements, emitting Wants for both storage and remote effects.
- Added the three-way merge against a stored base, with element-wise flag merging (flags never conflict, divergent sets fold into their union of changes) and content conflicts kept both, carrying the observed remote revision for the consumer to merge and resolve with an edit.
- Added edit-beats-delete in both directions: a remote update resurrects a local tombstone, and a local staged edit survives a remote delete as a pending create re-uploading the edited body.
- Added optimistic-concurrency content pushes gated on the last-synced revision (if_match), following the confirm-before-rewrite discipline: no local state is rewritten until the remote accepts the push.
- Added the rekey verb, rebuilding a collection after a handle-space change (an IMAP UIDVALIDITY bump) and carrying the cache and pending local state over to the new handles by link id.
- Added the std client behind the client feature: a blocking OfflineClient servicing every yield through the consumer-implemented Storage and Remote traits.
- Documented the at-least-once push contract (an add's link_id dedups a retry, a remove of an already-missing member reads as accepted) and the pointer-derived object refcounting the consumer maintains by diffing placement upserts and drops.

[unreleased]: https://github.com/pimalaya/io-offline/compare/v0.1.0..HEAD
[0.1.0]: https://github.com/pimalaya/io-offline/compare/root..v0.1.0
