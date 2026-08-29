---
cairn: log
change: a-replayed-push-resurrects-a-moved-source
landed: 2026-08-29
---

# A replayed push resurrects a moved source

`a_crashed_write_never_loses_data` failed at a raised case count on a move intent the ledger held nobody had voided, whose link id ended up in neither collection. It is a model bug, not an engine one, and the case itself says so: the same operations with the same injected crash, minus only the trailing local delete, pass at every crash point, and the engine leaves the member in the inbox alive and clean with the edited body on the server.

The mechanism is two engine rules meeting the at-least-once contract. A move whose source carries a staged edit pushes the edit ahead of the remove, so the relocated member carries the edited body rather than the one it replaced, and the remove derives again next run once the base holds what was pushed. The crash falls between that update being serviced and the write recording it. The next run enumerates a revision the tombstone's base does not name, and an enumerate carries a revision and no body, so the replica cannot separate its own replayed echo from a stranger's edit. Edit-beats-delete wins, the tombstone is replaced by a fresh pull, and the move is abandoned with the member back where it started. The user then deleted that resurrected member, which is a strictly later action on the same item and supersedes the move it displaced.

## What landed

The ledger voids a staged move when a later local delete removes the source it was staged on, beside the edit and flag claims that delete already voided. A move tombstones its own source, so a source pickable again is one the engine put back, and the guard fires nowhere else. The saved seed stays pinned, and the whole property file is green at 20000 cases.

No production code moved, so there is no changelog entry: nothing user-visible changed.

## Capabilities moved

- **sync**: a lost push record can abandon a move, leaving the member in its source collection rather than half-moved.
