use crate::db::Cache;
use crate::lock::Lock;

use crate::transaction::PAGE_LEN;
use crate::utils::First;

use crate::Result;
use serde::{Deserialize, Serialize};
use spin::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::fs::File;

use std::io::{Read, Seek, SeekFrom, Write};

use std::sync::Arc;
use std::usize;

pub struct State {
    pub writer: Mutex<VersionedState>,
    pub public: PublicState,
}

#[derive(Clone)]
pub struct PublicState {
    pub reader: Arc<RwLock<VersionedState>>,
    pub cache: Arc<RwLock<Cache>>,
    pub lock: Arc<Lock>,
}

// impl State{
//     pub fn folk(&self)->Self{
//         Self{
//             writer: Mutex::new(self.reader.lock().clone()),
//             reader: self.reader.clone()
//         }
//     }
// }

#[derive(Clone)]
pub struct VersionedState {
    pub indexes: BTreeMap<String, Index>,
    pub dirty: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Index {
    pub offset: u64,
    pub length: u64,
}

pub struct StateBuilder {
    pub file: Arc<RwLock<File>>,
}

impl StateBuilder {
    pub fn build(&self) -> Result<VersionedState> {
        let indexes = self.recover()?;
        Ok(VersionedState {
            indexes,
            dirty: false,
        })
    }

    pub fn recover(&self) -> Result<BTreeMap<String, Index>> {
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
                file.seek(SeekFrom::Start(position))?;
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
            while total > 0 {
                if !first.first() {
                    let mut buf = [1_u8; 1];
                    file.read_exact(&mut buf[..])?;
                    assert_eq!(buf[0], 0);
                }
                let header_len = first.get();
                let page_rest = PAGE_LEN - header_len;
                let to_read = total.min(page_rest);
                file.read_exact(&mut bytes[offset as usize..(offset + to_read) as usize])?;
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
        self.file.set_len(len)?;
        self.file.seek(SeekFrom::Start(len))?;
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

pub struct DataWriter<'file> {
    pub file: &'file mut File,
    pub data: Arc<Vec<u8>>,
}

impl<'file> DataWriter<'file> {
    pub fn write(&mut self) -> Result<u64> {
        let len = self.file.metadata()?.len();
        let page_offset = len / PAGE_LEN * PAGE_LEN;
        let mut buf = [0_u8; 1];
        let need_header = {
            if page_offset != len && {
                self.file.seek(SeekFrom::Start(page_offset))?;
                self.file.read_exact(&mut buf[..])?;
                buf[0] != 2
            } {
                let new_offset = ((len + PAGE_LEN - 1) / PAGE_LEN) * PAGE_LEN;
                self.file.set_len(new_offset)?;
                self.file.seek(SeekFrom::Start(new_offset))?;
                true
            } else {
                self.file.seek(SeekFrom::Start(len))?;
                len == page_offset
            }
        };
        if need_header {
            self.file.write(&[2_u8])?;
        }
        let data_offset = self.file.metadata()?.len();
        let mut total = self.data.len();
        let mut offset = 0;
        let rest = ((data_offset + PAGE_LEN - 1) / PAGE_LEN) * PAGE_LEN - data_offset;
        let mut first = First::new(rest, 1);
        while total > 0 {
            if !first.first() {
                self.file.write(&[2_u8])?;
            }
            let header_len = first.get();
            let page_rest = PAGE_LEN - header_len;
            let to_write = total.min(page_rest as usize);
            self.file.write(&self.data[offset..offset + to_write])?;
            offset += to_write;
            total -= to_write;
        }

        Ok(data_offset)
    }
}

pub struct DataRetriever<'file> {
    pub file: &'file mut File,
    pub offset: u64,
    pub length: u64,
}

impl<'file> DataRetriever<'file> {
    pub fn retrieve(&mut self) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(self.offset))?;
        let mut total = self.length;
        let mut bytes = vec![0_u8; total as usize];
        let rest = ((self.offset + PAGE_LEN - 1) / PAGE_LEN) * PAGE_LEN - self.offset;
        let mut first = First::new(rest, 1);
        let mut offset = 0;
        while total > 0 {
            if !first.first() {
                let mut buf = [1_u8; 1];
                self.file.read_exact(&mut buf[..])?;
                assert_eq!(buf[0], 2);
            }
            let header_len = first.get();
            let page_rest = PAGE_LEN - header_len;
            let to_read = total.min(page_rest);
            self.file
                .read_exact(&mut bytes[offset as usize..(offset + to_read) as usize])?;
            total -= to_read;
            offset += to_read;
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod test {
    use crate::state::{DataRetriever, DataWriter, Index, StateBuilder, StateWriter};
    use spin::RwLock;
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;
    use tempfile::tempfile;

    #[test]
    fn test_state_builder() {
        let file = tempfile().unwrap();
        let file = Arc::new(RwLock::new(file));
        let builder = StateBuilder { file: file.clone() };
        let state = Arc::new(RwLock::new(builder.build().unwrap()));
        assert_eq!(state.read().indexes.len(), 0);
        {
            let mut file_guard = file.write();
            let mut state_writer = state.write();

            for i in 0..100 {
                let value = format!("value{i}");
                let length = value.len() as u64;
                let mut data_writer = DataWriter {
                    file: file_guard.deref_mut(),
                    data: Arc::new(value.into_bytes()),
                };
                let offset = data_writer.write().unwrap();
                state_writer
                    .indexes
                    .insert(format!("key{i}"), Index { offset, length });
            }

            let mut writer = StateWriter {
                file: file_guard.deref_mut(),
                state: state_writer.deref(),
            };
            writer.write().unwrap();
        }
        let indexes = builder.recover().unwrap();
        {
            let mut file_guard = file.write();
            for i in 0..100 {
                let index = indexes.get(format!("key{i}").as_str()).unwrap().clone();
                let mut retriever = DataRetriever {
                    file: file_guard.deref_mut(),
                    offset: index.offset,
                    length: index.length,
                };
                let data = retriever.retrieve().unwrap();
                assert_eq!(*data, format!("value{i}").into_bytes());
            }
        }
    }
}
