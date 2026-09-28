#![no_std]
#![feature(generic_const_exprs)]
#![feature(allocator_api)]
#![allow(incomplete_features)]

pub mod device;
pub mod flash;
pub mod logger;
pub mod mqtt;
pub mod wifi;

pub use ivy_macros as macros;
pub use ivy_types as types;
pub use ivy_types::actor;

pub use paste;

extern crate alloc;

#[macro_export]
macro_rules! mk_static {
    ($t:ty, $val:expr) => {{
        static STATIC_CELL: static_cell::StaticCell<$t> = static_cell::StaticCell::new();
        STATIC_CELL.init(($val))
    }};
}
