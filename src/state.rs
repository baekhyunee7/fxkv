use crate::utils::First;
use crate::Result;
use std::fs::File;
use std::io::SeekFrom::Current;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::AtomicUsize;
use std::usize;

const MAX_DATA_LEN: u64 = u32::MAX as u64;

pub struct State {}

pub struct TreeState {}

// #[derive(Debug, Default)]
// pub struct TransactionState {
//     pub transaction_id: AtomicUsize,
// }
//
// impl TransactionState {
//     pub fn recover(file: &mut File) -> Result<Self> {
//         let len = file.metadata()?.len();
//         let mut len = ((len + PAGE_LEN - 1) / PAGE_LEN) * PAGE_LEN;
//         if len == 0 {
//             Ok(Self::default())
//         } else {
//             let mut buf = [0_u8; 1];
//             // find data header
//             while let Some(position) = len.checked_sub(PAGE_LEN as u64) {
//                 len = position;
//                 file.seek(SeekFrom::Start(position));
//                 file.read_exact(&mut buf[..])?;
//                 if buf[0] == 1_u8 {
//                     break;
//                 }
//             }
//             // data_len
//             let mut buf = [0_u8; 4];
//             file.read_exact(&mut buf[..])?;
//             let mut total = u32::from_be_bytes(buf) as u64;
//             // transaction_id
//             let mut buf = [0_u8; 8];
//             file.read_exact(&mut buf[..])?;
//             let mut transaction_id = usize::from_be_bytes(buf);
//             let mut bytes = vec![0_u8; total as usize];
//             let mut first = First::new(13, 1);
//             let mut offset = 0;
//             // file.seek(SeekFrom::Start(len));
//             while total > 0 {
//                 if !first.first() {
//                     let mut buf = [1_u8; 1];
//                     file.read_exact(&mut buf[..])?;
//                     assert_eq!(buf[0], 0);
//                 }
//                 let header_len = first.get();
//                 let page_rest = PAGE_LEN - header_len;
//                 let to_read = total.min(page_rest);
//                 file.read_exact(&mut bytes[offset as usize..(offset + to_read) as usize]);
//                 total -= to_read;
//                 offset += to_read;
//             }
//             let _ = TransactionData {
//                 transaction_id,
//                 data: bytes,
//             };
//             Ok(Self {
//                 transaction_id: AtomicUsize::from(transaction_id),
//             })
//         }
//     }
// }
//
