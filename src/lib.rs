//#![allow(unused_imports)]
//#![allow(unused_variables)]
//#![allow(unreachable_code)]
//#![allow(dead_code)]

#![feature(io_error_other)]
#![feature(drain_filter)]
#![feature(type_alias_impl_trait)]
#![feature(box_into_inner)]
#![feature(round_char_boundary)]
#![feature(hash_drain_filter)]

mod data_frame;

pub use data_frame::{Data, DataFrame, Groups, SimpleDateTime};
