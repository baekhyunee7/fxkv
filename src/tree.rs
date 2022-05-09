use crate::state::State;
use spin::mutex::Mutex;
use spin::MutexGuard;
use std::sync::Arc;

#[derive(Clone)]
pub struct Tree {
    pub state: Arc<State>,
    pub name: String,
}

impl Tree {
    pub fn set<K, V>(&self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        todo!()
    }
}

pub struct TransactionTrees {
    pub trees: Vec<Tree>,
}
