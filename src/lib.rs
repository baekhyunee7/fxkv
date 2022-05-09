extern crate core;

use crate::error::Error;

mod db;
mod error;
mod lock;
mod lru_map;
mod state;
mod thread_pool;
mod transaction;
mod tree;
mod utils;

pub type Result<T> = std::result::Result<T, Error>;
