use crate::state::State;
use crate::transaction::{TransactionBatch, TransactionBatchBuilder};
use crate::tree::Tree;
use crate::Result;
use spin::rwlock::RwLock;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub const TRANSACTION_FILE: &str = "db.transaction";

pub struct Db {
    pub file_manager: FileManager,
    pub context: Context,
    pub states: RwLock<HashMap<String, Arc<State>>>,
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
        todo!()
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
    fn file_name(name: &str) -> String {
        format!("{}.tree", name)
    }

    pub fn get_or_insert(&self, name: &str) -> Result<Arc<RwLock<File>>> {
        let mut files_guard = self.files.upgradeable_read();
        let file = {
            let path = Path::new(name);
            if path.exists() && path.is_file() {
                OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create(true)
                    .open(name)
            } else {
                OpenOptions::new()
                    .write(true)
                    .read(true)
                    .append(true)
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
