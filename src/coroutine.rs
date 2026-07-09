//! # Generator-shape coroutine driver
//!
//! Mirrors the shape of `core::ops::Coroutine`: a `Yield` associated type
//! for intermediate progress, a `Return` associated type for terminal
//! output, and a two-variant [`OfflineCoroutineState`] (`Yielded` /
//! `Complete`).
//!
//! Every verb in this crate (open, upgrade, mutate, sync) picks the
//! standard [`OfflineYield`] directly: it gathers every effect the engine
//! emits, both remote (enumerate, fetch, push) and storage (load, lookup
//! object, write). The engine performs none of them; a consumer
//! services each yield and resumes the coroutine with the matching
//! [`OfflineArg`].
//!
//! Storage is therefore not a trait injected into the engine, which would
//! break the I/O-free contract: it is `Wants` variants like everything
//! else. The optional std [`crate::client`] is one such consumer.

use alloc::{collections::BTreeMap, vec::Vec};

use crate::{
    change::{Change, WriteOp},
    collection::{Checkpoint, CollectionId},
    object::Hash,
    placement::{Handle, LinkId},
    remote::{FetchedItem, PushResult, RemoteSnapshot, Tier},
    storage::Loaded,
};

/// State yielded by an [`OfflineCoroutine::resume`] step.
///
/// Two-variant by design (matches std's `core::ops::CoroutineState`): any
/// further variation lives inside the per-coroutine `Yield` type.
#[derive(Debug)]
pub enum OfflineCoroutineState<Y, R> {
    /// Intermediate yield. The driver reacts to `Y` (read or write
    /// storage, talk to the remote) and resumes the coroutine again.
    Yielded(Y),
    /// Terminal yield. By convention `R = Result<Output, Error>`.
    Complete(R),
}

/// Standard-shape offline coroutine.
///
/// Implementors own their internal state machine and declare their
/// terminal `Return`. The driver reacts to each [`OfflineYield`] variant
/// and resumes until `Complete`.
pub trait OfflineCoroutine {
    /// Intermediate value handed back on every step; always
    /// [`OfflineYield`] in this crate.
    type Yield;
    /// Terminal value. By convention `Result<Output, Error>`.
    type Return;

    /// Advances the coroutine one step.
    ///
    /// Pass [`None`] on the initial call. Pass `Some(arg)` carrying the
    /// value matching the previous `Yielded` variant.
    fn resume(
        &mut self,
        arg: Option<OfflineArg>,
    ) -> OfflineCoroutineState<Self::Yield, Self::Return>;
}

/// Standard offline Yield. Every verb picks `type Yield = OfflineYield`.
///
/// Each variant is paired with the matching [`OfflineArg`] variant the
/// driver feeds back on the next `resume`. The first group is the remote
/// seam, the second the storage seam.
#[derive(Debug)]
pub enum OfflineYield {
    /// Consumer must enumerate the remote collection (full, or delta from
    /// `cursor`) and feed back [`OfflineArg::Enumerate`].
    WantsEnumerate {
        /// The collection to enumerate.
        collection: CollectionId,
        /// The last checkpoint to delta from, if any.
        cursor: Option<Checkpoint>,
    },

    /// Consumer must fetch each handle at `tier` and feed back
    /// [`OfflineArg::Fetch`].
    WantsFetch {
        /// The owning collection.
        collection: CollectionId,
        /// The handles to fetch.
        handles: Vec<Handle>,
        /// The detail tier.
        tier: Tier,
    },

    /// Consumer must push each change and feed back
    /// [`OfflineArg::Push`].
    WantsPush {
        /// The owning collection.
        collection: CollectionId,
        /// The changes to push.
        changes: Vec<Change>,
    },

    /// Consumer must load the collection's placements and checkpoint and
    /// feed back [`OfflineArg::Load`].
    WantsLoad(CollectionId),

    /// Consumer must resolve which link ids already have a stored object
    /// and feed back [`OfflineArg::LookupObject`]. This is the dedup
    /// check that skips re-downloading a body shared across collections.
    WantsLookupObject(Vec<LinkId>),

    /// Consumer must apply each write atomically and feed back
    /// [`OfflineArg::Write`].
    WantsWrite(Vec<WriteOp>),
}

/// Reply fed back into [`OfflineCoroutine::resume`] by the driver.
///
/// Each variant matches the corresponding [`OfflineYield`] request and
/// carries the value the driver gathered while servicing it.
#[derive(Clone, Debug)]
pub enum OfflineArg {
    /// Reply to [`OfflineYield::WantsEnumerate`].
    Enumerate(RemoteSnapshot),
    /// Reply to [`OfflineYield::WantsFetch`].
    Fetch(Vec<FetchedItem>),
    /// Reply to [`OfflineYield::WantsPush`].
    Push(Vec<PushResult>),
    /// Reply to [`OfflineYield::WantsLoad`].
    Load(Loaded),
    /// Reply to [`OfflineYield::WantsLookupObject`]: the subset of link
    /// ids that already map to a stored object.
    LookupObject(BTreeMap<LinkId, Hash>),
    /// Reply to [`OfflineYield::WantsWrite`].
    Write,
}
