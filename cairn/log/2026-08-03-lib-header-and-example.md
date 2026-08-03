---
cairn: log
change: lib-header-and-example
landed: 2026-08-03
---

# Domain-neutral lib.rs header, joined doc lines, first runnable example

The lib.rs header dropped its stale mail-first framing (contacts already run on the engine via cardamum) for "mailboxes of messages, address books of contacts, calendars of events", and every paragraph was briefly joined onto one long line to trial the markdown no-hard-wrap style in rustdoc. The trial lost: wrapped reads better, so the header was re-wrapped at 80 columns (the inline-002 rule stands, with reference-link URL definitions exempt), and the rustfmt-blessed pair is confirmed as the house style: code at rustfmt's default 100, prose at 80.

A new paragraph in "The five verbs" answers a real reader question: every verb is collection-scoped because the collection is the unit of consistency (spine completeness, checkpoint, merge), there is deliberately no cross-collection read verb, and the merged deduplicated view is a plain query the consumer runs against its own storage over shared link ids; open is the reference single-collection read, not the only read path.

Added examples/mailbox_lifecycle.rs, the first runnable example: in-memory storage and fake IMAP-like server, walking probe, offline open, meta and full hydration, cross-collection dedup (the archive copy links the stored object with zero extra downloads), an offline flag mutation, its confirmed push, and a remote-flag pull, printing each step. Cargo gained the example block and the README Examples section now redirects to ./examples per readme-012. Verified: the example runs and prints the expected counters, 146 tests pass, clippy and fmt clean.

No spec requirement moved; docs and examples only.
