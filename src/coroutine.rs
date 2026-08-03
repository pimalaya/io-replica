//! # Generator-shape coroutine driver
//!
//! Mirrors the shape of `core::ops::Coroutine`: a `Yield` associated type
//! for intermediate progress, a `Return` associated type for terminal
//! output, and a two-variant [`ReplicaCoroutineState`] (`Yielded` /
//! `Complete`).
//!
//! Every verb in this crate (open, upgrade, mutate, sync) picks the
//! standard [`ReplicaYield`] directly: it gathers every effect the engine
//! emits, both remote (enumerate, fetch, push) and storage (load, lookup
//! object, write). The engine performs none of them; a consumer
//! services each yield and resumes the coroutine with the matching
//! [`ReplicaArg`].
//!
//! Storage is therefore not a trait injected into the engine, which
//! would break the I/O-free contract: it is `Wants` variants like
//! everything else. The blocking [`crate::client`] is one such consumer.

use alloc::{collections::BTreeMap, vec::Vec};

use crate::{
    change::{ReplicaChange, ReplicaWriteOp},
    collection::{ReplicaCheckpoint, ReplicaCollectionId},
    object::ReplicaHash,
    placement::{ReplicaHandle, ReplicaLinkId},
    remote::{ReplicaFetchedItem, ReplicaPushResult, ReplicaRemoteSnapshot, ReplicaTier},
    storage::ReplicaLoaded,
};

/// State yielded by an [`ReplicaCoroutine::resume`] step.
///
/// Two-variant by design (matches std's `core::ops::CoroutineState`): any
/// further variation lives inside the per-coroutine `Yield` type.
#[derive(Debug)]
pub enum ReplicaCoroutineState<Y, R> {
    /// Intermediate yield. The driver reacts to `Y` (read or write
    /// storage, talk to the remote) and resumes the coroutine again.
    Yielded(Y),
    /// Terminal yield. By convention `R = Result<Output, Error>`.
    Complete(R),
}

/// Standard-shape offline coroutine.
///
/// Implementors own their internal state machine and declare their
/// terminal `Return`. The driver reacts to each [`ReplicaYield`] variant
/// and resumes until `Complete`.
pub trait ReplicaCoroutine {
    /// Intermediate value handed back on every step; always
    /// [`ReplicaYield`] in this crate.
    type Yield;
    /// Terminal value. By convention `Result<Output, Error>`.
    type Return;

    /// Advances the coroutine one step.
    ///
    /// Pass [`None`] on the initial call. Pass `Some(arg)` carrying the
    /// value matching the previous `Yielded` variant.
    fn resume(
        &mut self,
        arg: Option<ReplicaArg>,
    ) -> ReplicaCoroutineState<Self::Yield, Self::Return>;
}

/// Standard offline Yield. Every verb picks `type Yield = ReplicaYield`.
///
/// Each variant is paired with the matching [`ReplicaArg`] variant the
/// driver feeds back on the next `resume`. The first group is the remote
/// seam, the second the storage seam.
#[derive(Debug)]
pub enum ReplicaYield {
    /// Consumer must enumerate the remote collection (full, or delta from
    /// `cursor`) and feed back [`ReplicaArg::Enumerate`].
    WantsEnumerate {
        /// The collection to enumerate.
        collection: ReplicaCollectionId,
        /// The last checkpoint to delta from, if any.
        cursor: Option<ReplicaCheckpoint>,
    },
    /// Consumer must fetch each handle at `tier` and feed back
    /// [`ReplicaArg::Fetch`].
    WantsFetch {
        /// The owning collection.
        collection: ReplicaCollectionId,
        /// The handles to fetch.
        handles: Vec<ReplicaHandle>,
        /// The detail tier.
        tier: ReplicaTier,
    },
    /// Consumer must push each change and feed back
    /// [`ReplicaArg::Push`].
    WantsPush {
        /// The owning collection.
        collection: ReplicaCollectionId,
        /// The changes to push.
        changes: Vec<ReplicaChange>,
    },
    /// Consumer must load the collection's placements and checkpoint and
    /// feed back [`ReplicaArg::Load`].
    WantsLoad(ReplicaCollectionId),
    /// Consumer must resolve which link ids already have a stored object
    /// and feed back [`ReplicaArg::LookupObject`]. This is the dedup
    /// check that skips re-downloading a body shared across collections.
    WantsLookupObject(Vec<ReplicaLinkId>),
    /// Consumer must apply each write atomically and feed back
    /// [`ReplicaArg::Write`].
    WantsWrite(Vec<ReplicaWriteOp>),
}

/// Reply fed back into [`ReplicaCoroutine::resume`] by the driver.
///
/// Each variant matches the corresponding [`ReplicaYield`] request and
/// carries the value the driver gathered while servicing it.
#[derive(Clone, Debug)]
pub enum ReplicaArg {
    /// Reply to [`ReplicaYield::WantsEnumerate`].
    Enumerate(ReplicaRemoteSnapshot),
    /// Reply to [`ReplicaYield::WantsFetch`].
    Fetch(Vec<ReplicaFetchedItem>),
    /// Reply to [`ReplicaYield::WantsPush`].
    Push(Vec<ReplicaPushResult>),
    /// Reply to [`ReplicaYield::WantsLoad`].
    Load(ReplicaLoaded),
    /// Reply to [`ReplicaYield::WantsLookupObject`]: the subset of link
    /// ids that already map to a stored object.
    LookupObject(BTreeMap<ReplicaLinkId, ReplicaHash>),
    /// Reply to [`ReplicaYield::WantsWrite`].
    Write,
}
