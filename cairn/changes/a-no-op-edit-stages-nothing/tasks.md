---
cairn: tasks
change: a-no-op-edit-stages-nothing
---

- [x] Reproduce the shape as a failing test in tests/property.rs
- [x] Guard the `Edit` dirtying on the object having changed
- [x] Keep a conflict resolution dirty whatever body it carries
- [x] Unit-test both in src/mutate.rs beside the existing edit tests
- [x] Stop the model claiming an edit intent nothing staged
- [x] Fold the delta into cairn/spec/mutate.md, log, changelog
