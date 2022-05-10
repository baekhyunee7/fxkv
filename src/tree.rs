use crate::db::{Db, FileManager};
use crate::lock::Lock;
use crate::state::{DataRetriever, DataWriter, Index, State};
use crate::Result;
use spin::mutex::Mutex;
use spin::MutexGuard;
use std::ops::DerefMut;
use std::process::id;
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
    pub fn set<K>(&self, key: K, value: Vec<u8>) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = Arc::new(value);
        let tree = self.trees.trees.get(self.idx).unwrap();
        let file_name = FileManager::file_name(tree.name.as_str());
        let file = self
            .trees
            .db
            .file_manager
            .get_or_insert(file_name.as_str())?;
        {
            let mut file = file.write();
            let mut data_writer = DataWriter {
                file: file.deref_mut(),
                data: value.clone(),
            };
            let offset = data_writer.write()?;
            drop(file);
            let mut guard = tree.state.writer.lock();
            guard.indexes.insert(
                unsafe { std::str::from_utf8_unchecked(key) }.to_owned(),
                Some(Index {
                    offset,
                    length: value.len() as u64,
                }),
            );
        }
        Ok(())
    }

    pub fn get<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let tree = self.trees.trees.get(self.idx).unwrap();
        let key = key.as_ref();
        let index = {
            let mut guard = tree.state.writer.lock();
            if let Some(Some(value)) = guard
                .indexes
                .get(unsafe { std::str::from_utf8_unchecked(key) })
            {
                Some(value.clone())
            } else {
                None
            }
        };
        let value: Result<Option<Vec<u8>>> = index
            .map(|idx| {
                let mut cache = tree.state.cache.write();
                if let Some(value) = cache.get(&(idx.offset as usize)) {
                    Ok(value.clone())
                } else {
                    drop(cache);
                    let file_name = FileManager::file_name(tree.name.as_str());
                    let file = self
                        .trees
                        .db
                        .file_manager
                        .get_or_insert(file_name.as_str())?;
                    let mut file = file.write();
                    let mut retriever = DataRetriever {
                        file: file.deref_mut(),
                        offset: idx.offset,
                        length: idx.length,
                    };
                    let value = retriever.retrieve()?;
                    Ok(value)
                }
            })
            .transpose();
        Ok(value?)
    }

    pub fn remove<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let tree = self.trees.trees.get(self.idx).unwrap();
        let key = key.as_ref();
        let value = self.get(key)?;
        let mut guard = tree.state.writer.lock();
        guard
            .indexes
            .remove(unsafe { std::str::from_utf8_unchecked(key) });
        Ok(value)
    }
}

#[cfg(test)]
mod test {
    use crate::db::Db;

    #[test]
    fn test_transaction() {
        let db = Db::new().unwrap();
        db.open_tree("tree1").unwrap();
        db.open_tree("tree2").unwrap();
        let trees = db
            .start_transaction(["tree1", "tree2"].into_iter())
            .unwrap();
        let t1 = trees.get(0);
        let value1 = "value1".as_bytes().to_vec();
        t1.set("key1", value1.clone()).unwrap();
        assert_eq!(t1.get("key1").unwrap(), Some(value1.clone()));
        assert_eq!(t1.remove("key1").unwrap(), Some(value1.clone()));
        assert_eq!(t1.get("key1").unwrap(), None);
    }
}
