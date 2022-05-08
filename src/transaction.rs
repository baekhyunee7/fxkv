use crate::utils::{First, Windows};
use crate::{Error, Result};
use crossbeam::channel::{unbounded, Sender};
use gstuff::oneshot::oneshot;
use spin::RwLock;
use std::borrow::Borrow;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::DerefMut;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

const PAGE_LEN: u64 = 1024;

pub enum TransactionAction {
    Commit(TransactionCommitHandle),
    Drop(usize),
}

pub struct TransactionBatchBuilder {
    pub file: Arc<RwLock<File>>,
}

impl TransactionBatchBuilder {
    pub fn build(&mut self) -> Result<TransactionBatch> {
        let transaction_id = self.recover()?;
        let (sender, rx) = unbounded();
        let file = self.file.clone();
        let handle = thread::spawn(move || -> Result<()> {
            let mut windows = Windows::start_with(transaction_id + 1);
            let mut pending_transactions = vec![];
            while let Ok(action) = rx.recv() {
                match action {
                    TransactionAction::Commit(handle) => {
                        windows.put(handle.data.transaction_id);
                        pending_transactions.push(handle);
                    }
                    TransactionAction::Drop(id) => {
                        windows.put(id);
                    }
                }
                if windows.completed() {
                    let mut file = file.write();
                    for tran in pending_transactions.drain(..) {
                        let mut writer = TransactionWriter {
                            file: file.deref_mut(),
                            transaction_id: tran.data.transaction_id,
                            data: tran.data.data,
                        };
                        writer.write()?;
                        tran.sender.send(());
                    }
                }
            }
            Ok(())
        });
        Ok(TransactionBatch {
            transaction_id: AtomicUsize::new(transaction_id),
            sender: Some(sender),
            handle: Some(handle),
        })
    }

    pub fn recover(&mut self) -> Result<usize> {
        let mut file = self.file.write();
        let len = file.metadata()?.len();
        let mut len = ((len + PAGE_LEN - 1) / PAGE_LEN) * PAGE_LEN;
        if len == 0 {
            Ok(0)
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
            // data_len
            let mut buf = [0_u8; 4];
            file.read_exact(&mut buf[..])?;
            let mut total = u32::from_be_bytes(buf) as u64;
            // transaction_id
            let mut buf = [0_u8; 8];
            file.read_exact(&mut buf[..])?;
            let mut transaction_id = usize::from_be_bytes(buf);
            let mut bytes = vec![0_u8; total as usize];
            let mut first = First::new(13, 1);
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
            Ok(transaction_id)
        }
    }
}

pub struct TransactionData {
    pub transaction_id: usize,
    pub data: Option<Vec<u8>>,
}

pub struct TransactionCommitHandle {
    pub data: TransactionData,
    pub sender: gstuff::oneshot::Sender<()>,
}

pub struct TransactionWriter<'a> {
    pub file: &'a mut File,
    pub transaction_id: usize,
    pub data: Option<Vec<u8>>,
}

impl<'a> TransactionWriter<'a> {
    pub fn write(&mut self) -> Result<()> {
        let len = self.file.metadata()?.len();
        let len = ((len + PAGE_LEN - 1) / PAGE_LEN) * PAGE_LEN;
        self.file.set_len(len);
        self.file.seek(SeekFrom::Start(len));
        self.file.write(&[1_u8])?;
        let mut total = self.data.as_ref().map(|x| x.len()).unwrap_or(0);
        self.file.write(&(total as u32).to_be_bytes()[..])?;
        self.file.write(&self.transaction_id.to_be_bytes()[..])?;
        if let Some(data) = &self.data {
            let mut offset = 0;
            let mut first = First::new(13, 1);
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
        }
        Ok(())
    }
}

pub struct TransactionBatch {
    pub sender: Option<Sender<TransactionAction>>,
    pub handle: Option<JoinHandle<Result<()>>>,
    pub transaction_id: AtomicUsize,
}

impl Drop for TransactionBatch {
    fn drop(&mut self) {
        let sender = self.sender.take().unwrap();
        drop(sender);
        let handle = self.handle.take().unwrap();
        handle.join();
    }
}

impl TransactionBatch {
    pub fn commit(&self, data: TransactionData) -> Result<()> {
        let (sender, rx) = oneshot();
        let action = TransactionAction::Commit(TransactionCommitHandle { data, sender });
        self.sender.as_ref().unwrap().send(action)?;
        rx.recv().map_err(|err| Error::Unknown(err))?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::transaction::{TransactionBatchBuilder, TransactionData, TransactionWriter};
    use spin::RwLock;
    use std::sync::Arc;
    use tempfile::tempfile;

    #[test]
    fn test_transaction_writer() {
        let mut file = tempfile().unwrap();
        let mut writer = TransactionWriter {
            file: &mut file,
            transaction_id: 100,
            data: Some(vec![1_u8; 1050]),
        };
        writer.write().unwrap();
        let file = Arc::new(RwLock::new(file));
        let mut builder = TransactionBatchBuilder { file };
        let id = builder.recover().unwrap();
        assert_eq!(id, 100);
    }

    #[test]
    fn test_transaction_batch() {
        let file = tempfile().unwrap();
        let file = Arc::new(RwLock::new(file));
        let mut builder = TransactionBatchBuilder { file };
        let batch = builder.build().unwrap();
        for i in 1..=100 {
            batch
                .commit(TransactionData {
                    transaction_id: i,
                    data: None,
                })
                .unwrap();
        }
        let id = builder.recover().unwrap();
        assert_eq!(id, 100);
    }
}
