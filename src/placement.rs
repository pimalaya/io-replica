//! An item's presence in one collection: handle, link id, level, flags
//! and sync base.
//!
//! A placement is one of the two identity axes (see [`crate::object`] for
//! the other). It pins a logical item to a single collection through the
//! protocol [`OfflineHandle`], carries the per-location mutable state (flags,
//! membership), records the detail [`OfflineLevel`], and holds the [`OfflineBase`] the
//! three-way merge reconciles against.

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
};

use crate::{collection::OfflineCollectionId, object::OfflineHash};

/// The protocol's per-collection location of an item.
///
/// IMAP uidvalidity plus uid, WebDAV href, JMAP id; always a string so
/// non-integer ids are a non-issue. Identifies a placement within its
/// collection.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct OfflineHandle(pub String);

impl OfflineHandle {
    /// Borrows the handle as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for OfflineHandle {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for OfflineHandle {
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
pub struct OfflineLinkId(pub String);

impl OfflineLinkId {
    /// Borrows the link id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for OfflineLinkId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for OfflineLinkId {
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
pub struct OfflineMeta(pub String);

/// An item's set of state markers, normalized by the consumer.
///
/// A plain string set so the engine stays protocol-agnostic; the consumer
/// folds equivalent spellings (for example `\Seen`, `$seen`, `seen`)
/// before they reach here. Backends without per-item markers leave it
/// empty.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OfflineFlags(pub BTreeSet<String>);

impl OfflineFlags {
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
    pub fn merge(base: &OfflineFlags, local: &OfflineFlags, remote: &OfflineFlags) -> OfflineFlags {
        let kept = local.0.intersection(&remote.0).cloned();
        let local_adds = local.0.difference(&base.0).cloned();
        let remote_adds = remote.0.difference(&base.0).cloned();

        OfflineFlags(kept.chain(local_adds).chain(remote_adds).collect())
    }
}

impl<S: ToString> FromIterator<S> for OfflineFlags {
    fn from_iter<I: IntoIterator<Item = S>>(flags: I) -> Self {
        Self(flags.into_iter().map(|f| f.to_string()).collect())
    }
}

/// The detail level of a placement, a strict ladder where each rung
/// includes the one below.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum OfflineLevel {
    /// OfflineHandle known, nothing else; kept complete per collection.
    Probed,
    /// Minimal summary cached.
    Meta,
    /// Linked to a stored object body.
    Full,
}

/// How a placement relates to its sync base.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineStatus {
    /// In sync with the base; no pending push.
    Clean,
    /// Locally changed since the base; a push is pending.
    Dirty,
    /// Locally deleted since the base; a remove is pending.
    Tombstone,
    /// Content diverged on both sides; awaiting keep-both resolution.
    /// OfflineFlags never conflict (they merge element-wise), so this is a
    /// mutable-content state only.
    Conflict,
    /// Locally created (a copy, move or append) with no remote handle yet;
    /// a create is pending. Its [`OfflinePlacement::handle`] is a provisional
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
pub struct OfflineOrigin {
    /// The collection the source member lives in.
    pub collection: OfflineCollectionId,
    /// The source member's handle.
    pub handle: OfflineHandle,
}

/// The last-synced state a placement reconciles against.
///
/// Its existence is the membership base: a based placement was a member
/// of the collection as of the last sync (`OfflinePlacement::base` is `None`
/// until first reconciled). Where content is immutable only flags and
/// membership mutate; where it is mutable, `revision` holds the
/// last-synced content revision so an in-place edit is detected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineBase {
    /// Last-synced flag set.
    pub flags: OfflineFlags,
    /// Last-synced content revision for mutable-content backends (a WebDAV
    /// etag, an MS Graph changeKey); `None` where content is immutable.
    pub revision: Option<String>,
    /// Last-synced body, kept pinned in the object store (the consumer
    /// counts it as a reference) so a content three-way merge has its base
    /// bytes even after a local edit points the placement at a new object.
    /// `None` where content is immutable (the current object is always the
    /// synced one) or while no body has been synced.
    pub object: Option<OfflineHash>,
}

/// One item's presence in one collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflinePlacement {
    /// The collection this placement belongs to.
    pub collection: OfflineCollectionId,
    /// The protocol handle within that collection.
    pub handle: OfflineHandle,
    /// The cross-collection link id; `None` until [`OfflineLevel::Meta`].
    pub link_id: Option<OfflineLinkId>,
    /// The stored object body; `None` until [`OfflineLevel::Full`].
    pub object: Option<OfflineHash>,
    /// The current detail level.
    pub level: OfflineLevel,
    /// The cached summary; `None` until fetched. After a remote content
    /// change the sync drops the level back to [`OfflineLevel::Probed`] but
    /// keeps the stale summary as a display fallback until the next
    /// [`OfflineLevel::Meta`] upgrade refetches it.
    pub meta: Option<OfflineMeta>,
    /// The current flag set.
    pub flags: OfflineFlags,
    /// How this placement relates to its base.
    pub status: OfflineStatus,
    /// The remote content revision observed when the placement was marked
    /// [`OfflineStatus::Conflict`]: what the consumer resolves against (fetch that
    /// revision's body, merge, then edit). `None` otherwise.
    pub conflict_revision: Option<String>,
    /// The last-synced base; `None` until first reconciled.
    pub base: Option<OfflineBase>,
    /// For a [`OfflineStatus::Created`] placement, where its body already lives so
    /// the push can copy or move it; `None` otherwise (and for an append).
    pub origin: Option<OfflineOrigin>,
}

#[cfg(test)]
mod tests {
    use alloc::string::String;

    use crate::placement::{OfflineFlags, OfflineHandle, OfflineLinkId};

    fn flags(flags: &[&str]) -> OfflineFlags {
        OfflineFlags::from_iter(flags.iter().copied())
    }

    #[test]
    fn ids_convert_from_owned_and_borrowed_strings() {
        assert_eq!(
            OfflineHandle::from(String::from("1")),
            OfflineHandle::from("1")
        );
        assert_eq!(OfflineHandle::from("1").as_str(), "1");
        assert_eq!(
            OfflineLinkId::from(String::from("m")),
            OfflineLinkId::from("m")
        );
        assert_eq!(OfflineLinkId::from("m").as_str(), "m");
    }

    #[test]
    fn merge_keeps_unchanged_flags() {
        let base = flags(&["seen"]);
        let merged = OfflineFlags::merge(&base, &base, &base);
        assert_eq!(merged, base);
    }

    #[test]
    fn merge_takes_each_side_addition() {
        let merged = OfflineFlags::merge(&flags(&[]), &flags(&["a"]), &flags(&["b"]));
        assert_eq!(merged, flags(&["a", "b"]));
    }

    #[test]
    fn merge_takes_each_side_removal() {
        let base = flags(&["a", "b", "c"]);
        let merged = OfflineFlags::merge(&base, &flags(&["b", "c"]), &flags(&["a", "b"]));
        assert_eq!(merged, flags(&["b"]));
    }

    #[test]
    fn merge_removal_beats_the_other_side_keeping() {
        let base = flags(&["seen"]);
        let merged = OfflineFlags::merge(&base, &flags(&[]), &base);
        assert_eq!(merged, flags(&[]));
    }

    #[test]
    fn merge_agreeing_changes_converge() {
        // both sides added and removed the same flags: no double effect
        let base = flags(&["old"]);
        let both = flags(&["new"]);
        let merged = OfflineFlags::merge(&base, &both, &both);
        assert_eq!(merged, both);
    }
}
