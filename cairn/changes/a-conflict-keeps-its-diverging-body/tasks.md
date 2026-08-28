---
cairn: tasks
change: a-conflict-keeps-its-diverging-body
---

- [x] Add `conflict_object` to `ReplicaPlacement`, with the same lifetime as `conflict_revision`
- [x] Mark the diverging body wanted when a conflict is marked
- [x] Satisfy the want in the upgrade pass, reusing the claim-versus-payload rule
- [x] Drop the stored body when the tracked revision moves, in the same write
- [x] Clear both on resolution, alongside the existing base rebase
- [x] Test: a marked conflict asks for the diverging body
- [x] Test: a conflict whose remote moved again drops the body and asks anew
- [x] Test: resolving clears the pair
- [x] Test: an immutable-content backend still reaches none of this
