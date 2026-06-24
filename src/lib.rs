#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

#[macro_use]
extern crate alloc;
#[cfg(feature = "client")]
extern crate std;

pub mod change;
#[cfg(feature = "client")]
pub mod client;
pub mod collection;
pub mod coroutine;
pub mod mutate;
pub mod object;
pub mod open;
pub mod placement;
pub mod remote;
pub mod storage;
pub mod sync;
pub mod upgrade;
