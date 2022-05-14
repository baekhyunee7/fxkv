#[warn(dead_code)]
extern crate core;

use crate::error::Error;

pub mod db;
pub mod error;
pub mod lock;
pub mod lru_map;
pub mod state;
pub mod thread_pool;
pub mod transaction;
pub mod tree;
pub mod utils;

pub type Result<T> = std::result::Result<T, Error>;
