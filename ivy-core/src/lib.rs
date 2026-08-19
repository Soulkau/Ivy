#![no_std]
#![feature(generic_const_exprs)]
#![feature(unsafe_cell_access)]
#![allow(incomplete_features)]

pub mod bluetooth;
pub mod connection;
pub mod device;
pub mod logger;
pub mod mqtt;
pub mod storage;
pub mod wifi;

pub use ivy_macros as macros;
pub use ivy_types as types;
pub use ivy_types::actor;

pub use paste;
