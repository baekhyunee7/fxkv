use crate::lock::Lock;
use crate::lru_map::LruMap;
use crate::state::{PublicState, State, StateBuilder};
use crate::transaction::{TransactionBatch, TransactionBatchBuilder};
use crate::tree::{TransactionTrees, Tree};
use crate::Result;
use spin::mutex::Mutex;
use spin::rwlock::RwLock;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub const TRANSACTION_FILE: &str = "db.transaction";
pub type Cache = LruMap<usize, Vec<u8>, 1024>;

pub struct Db {
    pub file_manager: FileManager,
    pub context: Context,
    pub states: RwLock<HashMap<String, PublicState>>,
    pub batch: TransactionBatch,
}

impl Db {
    pub fn new() -> Result<Self> {
        let file_manager = FileManager::new();
        let file = file_manager.get_or_insert(TRANSACTION_FILE)?;
        let mut transaction_builder = TransactionBatchBuilder { file };
        let batch = transaction_builder.build()?;

        let this = Self {
            file_manager,
            context: Context {},
            states: RwLock::new(HashMap::new()),
            batch,
        };
        Ok(this)
    }

    pub fn open_tree(&self, name: &str) -> Result<Tree> {
        let mut guard = self.states.write();
        let state = guard
            .entry(name.to_owned())
            .or_insert({
                let file_name = FileManager::file_name(name);
                let file = self.file_manager.get_or_insert(file_name.as_str())?;
                let state_builder = StateBuilder { file };
                let version_state = state_builder.build()?;
                PublicState {
                    cache: Arc::new(RwLock::new(Cache::new())),
                    lock: Arc::new(Lock::new()),
                    reader: Arc::new(RwLock::new(version_state)),
                }
            })
            .clone();
        // drop(guard);
        Ok(Tree {
            state: State {
                public: state.clone(),
                writer: Mutex::new(state.reader.clone().read().clone()),
            },
            name: Arc::new(name.to_owned()),
        })
    }

    pub fn start_transaction<I>(&self, names: I) -> Result<TransactionTrees>
    where
        I: Iterator<Item = &'static str>,
    {
        let trees: Result<Vec<Tree>> = names.map(|name| self.open_tree(name)).collect();
        let trees = trees?;
        let mut locks: Vec<_> = trees
            .iter()
            .map(|x| (x.name.clone(), x.state.public.lock.clone()))
            .collect();
        locks.sort_by_key(|(name, _)| name.clone());
        Ok(TransactionTrees {
            trees,
            locks: locks
                .into_iter()
                .map(|(_, lock)| {
                    lock.lock()?;
                    Ok(lock)
                })
                .collect::<Result<Vec<Arc<Lock>>>>()?,
            committed: AtomicBool::new(false),
            db: self,
            transaction_id: self.batch.new_id(),
        })
    }
}

pub struct Context {}

pub struct FileManager {
    pub files: RwLock<HashMap<String, Arc<RwLock<File>>>>,
}

impl FileManager {
    pub fn new() -> Self {
        Self {
            files: RwLock::new(HashMap::new()),
        }
    }

    #[inline]
    pub fn file_name(name: &str) -> String {
        format!("{}.tree", name)
    }

    pub fn get_or_insert(&self, name: &str) -> Result<Arc<RwLock<File>>> {
        let files_guard = self.files.upgradeable_read();
        let file = {
            let path = Path::new(name);
            if path.exists() && path.is_file() {
                OpenOptions::new()
                    .write(true)
                    .read(true)
                    .append(true)
                    .open(name)
            } else {
                OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create(true)
                    .open(name)
            }
        }?;
        if let Some(result) = files_guard.get(name) {
            Ok(result.clone())
        } else {
            let mut files_guard = files_guard.upgrade();
            let result = Arc::new(RwLock::new(file));
            files_guard.insert(name.to_owned(), result.clone());
            Ok(result)
        }
    }
}
