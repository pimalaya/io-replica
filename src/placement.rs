//! An item's presence in one collection: handle, link id, level, flags
//! and sync base.
//!
//! A placement is one of the two identity axes (see [`crate::object`] for
//! the other). It pins a logical item to a single collection through the
//! protocol [`ReplicaHandle`], carries the per-location mutable state (flags,
//! membership), records the detail [`ReplicaLevel`], and holds the [`ReplicaBase`] the
//! three-way merge reconciles against.

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
};

use crate::{collection::ReplicaCollectionId, object::ReplicaHash};

/// The protocol's per-collection location of an item.
///
/// IMAP uidvalidity plus uid, WebDAV href, JMAP id; always a string so
/// non-integer ids are a non-issue. Identifies a placement within its
/// collection.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ReplicaHandle(pub String);

impl ReplicaHandle {
    /// Borrows the handle as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ReplicaHandle {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for ReplicaHandle {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// The logical-item identity used to group copies and map across
/// protocols.
///
/// A source global id (a provider message id, a JMAP id), else a stable
/// content id (the Message-ID header, the vCard or iCalendar UID). Never
/// derived from a size or other per-copy value a provider may rewrite.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ReplicaLinkId(pub String);

impl ReplicaLinkId {
    /// Borrows the link id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ReplicaLinkId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for ReplicaLinkId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// A minimal cached summary: enough for a list row and to resolve the
/// link id without a body.
///
/// Opaque to the engine (json in the reference store: sender, title, date
/// and the like; projected from the body where the backend has no cheap
/// summary tier).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaMeta(pub String);

/// An item's set of state markers, normalized by the consumer.
///
/// A plain string set so the engine stays protocol-agnostic; the consumer
/// folds equivalent spellings (for example `\Seen`, `$seen`, `seen`)
/// before they reach here. Backends without per-item markers leave it
/// empty.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaFlags(pub BTreeSet<String>);

impl ReplicaFlags {
    /// Reports whether `flag` is present.
    pub fn contains(&self, flag: &str) -> bool {
        self.0.contains(flag)
    }

    /// Three-way merges two flag sets element-wise against their base.
    ///
    /// Each flag is independent: the side that changed a flag's presence
    /// from the base wins for that flag, and both sides changing always
    /// agree (a presence flip from the base has only one direction), so
    /// an element-wise merge can never conflict. As set algebra the
    /// result is (local AND remote) OR (local MINUS base) OR (remote
    /// MINUS base).
    pub fn merge(base: &ReplicaFlags, local: &ReplicaFlags, remote: &ReplicaFlags) -> ReplicaFlags {
        let kept = local.0.intersection(&remote.0).cloned();
        let local_adds = local.0.difference(&base.0).cloned();
        let remote_adds = remote.0.difference(&base.0).cloned();

        ReplicaFlags(kept.chain(local_adds).chain(remote_adds).collect())
    }
}

impl<S: ToString> FromIterator<S> for ReplicaFlags {
    fn from_iter<I: IntoIterator<Item = S>>(flags: I) -> Self {
        Self(flags.into_iter().map(|f| f.to_string()).collect())
    }
}

/// The detail level of a placement, a strict ladder where each rung
/// includes the one below.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ReplicaLevel {
    /// ReplicaHandle known, nothing else; kept complete per collection.
    Probed,
    /// Minimal summary cached.
    Meta,
    /// Linked to a stored object body.
    Full,
}

/// How a placement relates to its sync base.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaStatus {
    /// In sync with the base; no pending push.
    Clean,
    /// Locally changed since the base; a push is pending.
    Dirty,
    /// Locally deleted since the base; a remove is pending.
    Tombstone,
    /// Content diverged on both sides; awaiting keep-both resolution.
    /// ReplicaFlags never conflict (they merge element-wise), so this is a
    /// mutable-content state only.
    Conflict,
    /// Locally created (a copy, move or append) with no remote handle yet;
    /// a create is pending. Its [`ReplicaPlacement::handle`] is a provisional
    /// placeholder until the push reconciles it to the server-assigned one.
    Created,
}

/// Where a pending create's content already lives, so the push can reuse a
/// server-side copy or move instead of re-uploading the body.
///
/// `None` on the placement means a genuine append (new content the server
/// has never seen); `Some` means the body is already a member of `collection`
/// under `handle`, so the push issues a copy or move from there.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaOrigin {
    /// The collection the source member lives in.
    pub collection: ReplicaCollectionId,
    /// The source member's handle.
    pub handle: ReplicaHandle,
}

/// The last-synced state a placement reconciles against.
///
/// Its existence is the membership base: a based placement was a member
/// of the collection as of the last sync (`ReplicaPlacement::base` is `None`
/// until first reconciled). Where content is immutable only flags and
/// membership mutate; where it is mutable, `revision` holds the
/// last-synced content revision so an in-place edit is detected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBase {
    /// Last-synced flag set.
    pub flags: ReplicaFlags,
    /// Last-synced content revision for mutable-content backends (a WebDAV
    /// etag, an MS Graph changeKey); `None` where content is immutable.
    pub revision: Option<String>,
    /// Last-synced body, kept pinned in the object store (the consumer
    /// counts it as a reference) so a content three-way merge has its base
    /// bytes even after a local edit points the placement at a new object.
    /// `None` where content is immutable (the current object is always the
    /// synced one) or while no body has been synced.
    pub object: Option<ReplicaHash>,
}

/// One item's presence in one collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaPlacement {
    /// The collection this placement belongs to.
    pub collection: ReplicaCollectionId,
    /// The protocol handle within that collection.
    pub handle: ReplicaHandle,
    /// The cross-collection link id; `None` until [`ReplicaLevel::Meta`].
    pub link_id: Option<ReplicaLinkId>,
    /// The stored object body; `None` until [`ReplicaLevel::Full`].
    pub object: Option<ReplicaHash>,
    /// The current detail level.
    pub level: ReplicaLevel,
    /// The cached summary; `None` until fetched. After a remote content
    /// change the sync drops the level back to [`ReplicaLevel::Probed`] but
    /// keeps the stale summary as a display fallback until the next
    /// [`ReplicaLevel::Meta`] upgrade refetches it.
    pub meta: Option<ReplicaMeta>,
    /// The current flag set.
    pub flags: ReplicaFlags,
    /// How this placement relates to its base.
    pub status: ReplicaStatus,
    /// The remote content revision observed when the placement was marked
    /// [`ReplicaStatus::Conflict`]: what the consumer resolves against (fetch that
    /// revision's body, merge, then edit). `None` otherwise.
    pub conflict_revision: Option<String>,
    /// The last-synced base; `None` until first reconciled.
    pub base: Option<ReplicaBase>,
    /// For a [`ReplicaStatus::Created`] placement, where its body already lives so
    /// the push can copy or move it; `None` otherwise (and for an append).
    pub origin: Option<ReplicaOrigin>,
}

#[cfg(test)]
mod tests {
    use alloc::string::String;

    use crate::placement::{ReplicaFlags, ReplicaHandle, ReplicaLinkId};

    fn flags(flags: &[&str]) -> ReplicaFlags {
        ReplicaFlags::from_iter(flags.iter().copied())
    }

    #[test]
    fn ids_convert_from_owned_and_borrowed_strings() {
        assert_eq!(
            ReplicaHandle::from(String::from("1")),
            ReplicaHandle::from("1")
        );
        assert_eq!(ReplicaHandle::from("1").as_str(), "1");
        assert_eq!(
            ReplicaLinkId::from(String::from("m")),
            ReplicaLinkId::from("m")
        );
        assert_eq!(ReplicaLinkId::from("m").as_str(), "m");
    }

    #[test]
    fn merge_keeps_unchanged_flags() {
        let base = flags(&["seen"]);
        let merged = ReplicaFlags::merge(&base, &base, &base);
        assert_eq!(merged, base);
    }

    #[test]
    fn merge_takes_each_side_addition() {
        let merged = ReplicaFlags::merge(&flags(&[]), &flags(&["a"]), &flags(&["b"]));
        assert_eq!(merged, flags(&["a", "b"]));
    }

    #[test]
    fn merge_takes_each_side_removal() {
        let base = flags(&["a", "b", "c"]);
        let merged = ReplicaFlags::merge(&base, &flags(&["b", "c"]), &flags(&["a", "b"]));
        assert_eq!(merged, flags(&["b"]));
    }

    #[test]
    fn merge_removal_beats_the_other_side_keeping() {
        let base = flags(&["seen"]);
        let merged = ReplicaFlags::merge(&base, &flags(&[]), &base);
        assert_eq!(merged, flags(&[]));
    }

    #[test]
    fn merge_agreeing_changes_converge() {
        // both sides added and removed the same flags: no double effect
        let base = flags(&["old"]);
        let both = flags(&["new"]);
        let merged = ReplicaFlags::merge(&base, &both, &both);
        assert_eq!(merged, both);
    }
}
