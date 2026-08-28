---
cairn: tasks
change: a-source-cannot-diverge-from-itself
---

- [x] Reproduce: a second offline edit is dropped as a conflict
- [x] Reproduce: a resolving edit on a conflicted binding never becomes the shared body
- [x] Add `shared_object` to `ReplicaSourceBinding`, the shared body this source last reconciled against
- [x] Compare the shared axis against it, falling back to the sync base until the source has folded once
- [x] Move the agreement point on every live absorb, and leave it where a tombstone found it
- [x] Assert the shared body wherever a conflict test asserts only flags
- [x] Test: a divergence between two unpushed edits still conflicts
- [x] Test: both cases end to end, through the engine over the hub harness
