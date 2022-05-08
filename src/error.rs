use crate::transaction::TransactionAction;
use crossbeam::channel::SendError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Commit Error: {0}")]
    Commit(#[from] SendError<TransactionAction>),
    #[error("Unknown Error:{0}")]
    Unknown(String),
}
