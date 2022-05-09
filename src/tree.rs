use crate::db::Db;
use crate::lock::Lock;
use crate::state::State;
use crate::Result;
use spin::mutex::Mutex;
use spin::MutexGuard;
use std::sync::Arc;

#[derive(Clone)]
pub struct Tree {
    pub state: Arc<State>,
    pub name: Arc<String>,
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

pub struct TransactionTrees<'a> {
    pub trees: Vec<Tree>,
    pub locks: Vec<Arc<Lock>>,
    pub committed: bool,
    pub db: &'a Db,
    pub transaction_id: usize,
}

impl<'a> TransactionTrees<'a> {
    pub fn get(&self, idx: usize) -> IndexedTransactionTrees {
        assert!(idx < self.trees.len());
        IndexedTransactionTrees { trees: self, idx }
    }
}

impl<'a> Drop for TransactionTrees<'a> {
    fn drop(&mut self) {
        if !self.committed {
            for lock in &self.locks {
                lock.unlock()
            }
            self.db.batch.drop(self.transaction_id);
        }
    }
}

pub struct IndexedTransactionTrees<'a, 'db> {
    pub trees: &'a TransactionTrees<'db>,
    pub idx: usize,
}

impl<'a, 'db> IndexedTransactionTrees<'a, 'db> {
    pub fn set<K>(key: K, value: Vec<u8>) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();

        Ok(())
    }
}
