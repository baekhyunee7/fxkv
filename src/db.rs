// use std::collections::HashMap;
// use std::fs::{File, OpenOptions};
// use std::path::{Path, PathBuf};
// use std::sync::Arc;
// use std::sync::atomic::AtomicUsize;
// use spin::rwlock::RwLock;
// use crate::tree::Tree;
// use crate::Result;
//
// pub struct  Db{
//     pub files: RwLock<HashMap<String,Arc<RwLock<File>>>>,
//     pub context: Context,
//     pub states: RwLock<HashMap<String,Arc<>>>
// }
//
//
// pub struct  FileContext{
//     pub file: File,
//
// }
//
// impl Db {
//     pub fn new()-> Self{
//
//     }
//
//     pub fn open_tree(&self,name:&str)-> Result<Tree>{
//         let file = self.get_or_insert(name)?;
//         let
//     }
//
//     fn get_or_insert(&self,name:&str)-> Result<&mut file>{
//         let mut files_guard = self.files.upgradeable_read();
//         let file = {
//             let path = Path::new(self.file_name(name).as_str());
//             if path.exists() && path.is_file(){
//                 OpenOptions::new()
//                     .write(true)
//                     .read(true)
//                     .create(true)
//                     .open(name)
//             }else{
//                 OpenOptions::new()
//                     .write(true)
//                     .read(true)
//                     .append(true)
//                     .open(name)
//             }
//         }?;
//         Ok(files_guard.entry(name.to_owned()).or_insert(file))
//     }
//
//
//     fn file_name(&self,name:&str)->String{
//         format!("{}.tree",name)
//     }
//
//     fn read_transaction(&self){
//
//     }
// }
//
// pub struct Transaction{
//     pub transaction_id: AtomicUsize
// }
//
// pub struct Context{
// }
