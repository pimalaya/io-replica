---
cairn: change
id: lib-header-and-example
status: landed
created: 2026-08-03
---

# Domain-neutral lib.rs header, joined doc lines, first runnable example

## Why

A full read of the crate raised three doc gaps: the header still framed the engine as mail-first ("mail first, contacts and calendar next", stale since cardamum runs contacts on it), it never explained why open is per-collection and how that squares with the merged across-collections view, and the crate had no concrete example to internalise what the library provides.

## What

- Rewrite the lib.rs header domain-neutral (mailboxes of messages, address books of contacts, calendars of events), with every paragraph joined onto one long line (the markdown-001 style, trialled against the inline-002 80-column rule).
- Add a paragraph making the collection scoping explicit: the collection is the unit of consistency, there is deliberately no cross-collection read verb, and the merged view is a plain consumer-side storage query over shared link ids.
- Add examples/mailbox_lifecycle.rs, a runnable end-to-end tour (probe, offline open, hydrate, dedup across collections, offline flag, push, pull) over an in-memory storage and fake server, with its Cargo example block and the README Examples redirect updated.
