//#![allow(unused_imports)]
//#![allow(unused_variables)]
//#![allow(unreachable_code)]
//#![allow(dead_code)]

#![feature(io_error_other)]
#![feature(drain_filter)]
#![feature(type_alias_impl_trait)]
#![feature(box_into_inner)]

mod data_frame;
use std::sync::Arc;

pub use data_frame::{Data, DataFrame as BlankDataFrame, Groups, SimpleDateTime};
pub type DataFrame = Arc<BlankDataFrame>;
