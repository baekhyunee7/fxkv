use crate::db::{Db, FileManager};
use crate::lock::Lock;
use crate::state::{DataRetriever, DataWriter, Index, State, StateWriter};
use crate::transaction::TransactionData;
use crate::Result;
use spin::mutex::Mutex;
use spin::MutexGuard;
use std::ops::{Bound, Deref, DerefMut, Range, RangeBounds};
use std::process::id;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct Tree {
    pub state: State,
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
    pub committed: AtomicBool,
    pub db: &'a Db,
    pub transaction_id: usize,
}

impl<'a> TransactionTrees<'a> {
    pub fn get(&self, idx: usize) -> IndexedTransactionTrees {
        assert!(idx < self.trees.len());
        IndexedTransactionTrees { trees: self, idx }
    }

    pub fn commit(&self) -> Result<()> {
        for tree in self.trees.iter() {
            let file_name = FileManager::file_name(tree.name.as_str());
            let file = self.db.file_manager.get_or_insert(file_name.as_str())?;
            let mut file = file.write();
            let mut state = tree.state.writer.lock();
            if state.dirty {
                let mut page_writer = StateWriter {
                    file: file.deref_mut(),
                    state: state.deref(),
                };
                page_writer.write()?;
                let mut reader = tree.state.public.reader.write();
                std::mem::swap(reader.deref_mut(), state.deref_mut());
                // *reader = state.clone()
            }
        }
        for lock in self.locks.iter() {
            lock.unlock();
        }
        self.committed.store(true, Ordering::SeqCst);
        self.db.batch.commit(TransactionData {
            data: None,
            transaction_id: self.transaction_id,
        })?;
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        self.committed.store(true, Ordering::SeqCst);
        for lock in self.locks.iter() {
            lock.unlock();
        }
        self.db.batch.drop(self.transaction_id)?;
        Ok(())
    }
}

impl<'a> Drop for TransactionTrees<'a> {
    fn drop(&mut self) {
        if !self.committed.load(Ordering::SeqCst) {
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
                Index {
                    offset,
                    length: value.len() as u64,
                },
            );
            guard.dirty = true;
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
            if let Some(value) = guard
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
                let mut cache = tree.state.public.cache.write();
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

    pub fn scan<K, R>(&self, keys: R) -> Result<Vec<Vec<u8>>>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let tree = self.trees.trees.get(self.idx).unwrap();
        let mut guard = tree.state.writer.lock();

        let lo = match keys.start_bound() {
            Bound::Included(k) => {
                Bound::Included(unsafe { std::str::from_utf8_unchecked(k.as_ref()) })
            }
            Bound::Excluded(k) => {
                Bound::Excluded(unsafe { std::str::from_utf8_unchecked(k.as_ref()) })
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let hi = match keys.end_bound() {
            Bound::Included(k) => {
                Bound::Included(unsafe { std::str::from_utf8_unchecked(k.as_ref()) })
            }
            Bound::Excluded(k) => {
                Bound::Excluded(unsafe { std::str::from_utf8_unchecked(k.as_ref()) })
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let ranges = guard.indexes.range::<str, _>((lo, hi));
        let mut cache = tree.state.public.cache.write();
        let values: Result<Vec<Vec<u8>>> = ranges
            .map(|(_, idx)| {
                if let Some(value) = cache.get(&(idx.offset as usize)) {
                    Ok(value.clone())
                } else {
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
            .collect();
        Ok(values?)
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
        guard.dirty = true;
        Ok(value)
    }
}

#[cfg(test)]
mod test {
    use crate::db::Db;
    use std::ops::Bound;

    #[test]
    fn test_transaction() {
        let db = Db::new().unwrap();
        // db.open_tree("tree1").unwrap();
        // db.open_tree("tree2").unwrap();
        let trees = db
            .start_transaction(["tree1", "tree2"].into_iter())
            .unwrap();
        let t1 = trees.get(0);
        let value1 = "value1".as_bytes().to_vec();
        t1.set("key1", value1.clone()).unwrap();
        assert_eq!(t1.get("key1").unwrap(), Some(value1.clone()));
        let range = t1
            .scan::<&str, _>((Bound::Included("key0"), Bound::Excluded("key2")))
            .unwrap();
        assert_eq!(range.len(), 1);
        assert_eq!(range[0], value1.clone());
        assert_eq!(t1.remove("key1").unwrap(), Some(value1.clone()));
        assert_eq!(t1.get("key1").unwrap(), None);
        // commit
        t1.set("key1", value1.clone()).unwrap();
        trees.commit().unwrap();
        let trees = db
            .start_transaction(["tree1", "tree2"].into_iter())
            .unwrap();
        let t1 = trees.get(0);
        assert_eq!(t1.get("key1").unwrap(), Some(value1.clone()));
    }
}
