use crate::lru_map::LruMap;
use crate::transaction::PAGE_LEN;
use crate::utils::First;
use crate::Error::Serde;
use crate::Result;
use spin::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::SeekFrom::Current;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::os::linux::raw::stat;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::usize;

pub struct State {
    pub reader: RwLock<VersionedState>,
    pub writer: Mutex<VersionedState>,
}

#[derive(Clone)]
pub struct VersionedState {
    pub indexes: BTreeMap<String, Option<usize>>,
}

pub struct StateBuilder {
    pub file: Arc<RwLock<File>>,
}

impl StateBuilder {
    pub fn build(&self) -> Result<State> {
        let indexes = self.recover()?;
        let state = VersionedState { indexes };
        Ok(State {
            reader: RwLock::new(state.clone()),
            writer: Mutex::new(state),
        })
    }

    pub fn recover(&self) -> Result<BTreeMap<String, Option<usize>>> {
        let mut file = self.file.write();
        let len = file.metadata()?.len();
        let mut len = ((len + PAGE_LEN - 1) / PAGE_LEN) * PAGE_LEN;
        if len == 0 {
            Ok(BTreeMap::new())
        } else {
            let mut buf = [0_u8; 1];
            // find data header
            while let Some(position) = len.checked_sub(PAGE_LEN as u64) {
                len = position;
                file.seek(SeekFrom::Start(position));
                file.read_exact(&mut buf[..])?;
                if buf[0] == 1_u8 {
                    break;
                }
            }
            let mut buf = [0_u8; 4];
            file.read_exact(&mut buf[..])?;
            let mut total = u32::from_be_bytes(buf) as u64;
            let mut bytes = vec![0_u8; total as usize];
            let mut first = First::new(5, 1);
            let mut offset = 0;
            // file.seek(SeekFrom::Start(len));
            while total > 0 {
                if !first.first() {
                    let mut buf = [1_u8; 1];
                    file.read_exact(&mut buf[..])?;
                    assert_eq!(buf[0], 0);
                }
                let header_len = first.get();
                let page_rest = PAGE_LEN - header_len;
                let to_read = total.min(page_rest);
                file.read_exact(&mut bytes[offset as usize..(offset + to_read) as usize]);
                total -= to_read;
                offset += to_read;
            }
            Ok(serde_json::from_slice(&bytes[..])?)
        }
    }
}

pub struct StateWriter<'a, 'file> {
    pub file: &'file mut File,
    pub state: &'a VersionedState,
}

impl<'a, 'file> StateWriter<'a, 'file> {
    pub fn write(&mut self) -> Result<()> {
        let len = self.file.metadata()?.len();
        let len = ((len + PAGE_LEN - 1) / PAGE_LEN) * PAGE_LEN;
        self.file.set_len(len);
        self.file.seek(SeekFrom::Start(len));
        self.file.write(&[1_u8])?;
        let data = serde_json::to_vec(&self.state.indexes)?;
        let mut total = data.len();
        self.file.write(&(total as u32).to_be_bytes()[..])?;
        let mut offset = 0;
        let mut first = First::new(5, 1);
        while total > 0 {
            if !first.first() {
                self.file.write(&[0_u8])?;
            }
            let header_len = first.get();
            let page_rest = PAGE_LEN - header_len;
            let to_write = total.min(page_rest as usize);
            self.file.write(&data[offset..offset + to_write])?;
            offset += to_write;
            total -= to_write;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::state::{State, StateBuilder, StateWriter};
    use spin::RwLock;
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;
    use tempfile::tempfile;

    #[test]
    fn test_state_builder() {
        let file = tempfile().unwrap();
        let mut file = Arc::new(RwLock::new(file));
        let builder = StateBuilder { file: file.clone() };
        let state = builder.build().unwrap();
        assert_eq!(state.writer.lock().indexes.len(), 0);
        {
            let mut writer = state.writer.lock();

            for i in 0..100 {
                writer.indexes.insert(format!("key{i}"), Some(i));
            }

            let mut file_guard = file.write();
            let mut writer = StateWriter {
                file: file_guard.deref_mut(),
                state: writer.deref(),
            };
            writer.write();
        }
        let indexes = builder.recover().unwrap();
        for i in 0..100 {
            assert_eq!(indexes.get(format!("key{i}").as_str()).unwrap(), &Some(i));
        }
    }
}
