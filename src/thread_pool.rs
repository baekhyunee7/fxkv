use crossbeam::channel::{unbounded, Sender};
use gstuff::oneshot::oneshot;
use std::panic::{catch_unwind, AssertUnwindSafe, UnwindSafe};
use std::thread;
use std::thread::JoinHandle;

pub type Worker = Box<dyn FnOnce() -> () + Send + 'static>;

pub struct ThreadPool {
    pub sender: Sender<Worker>,
    pub handles: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new(num: usize) -> Self {
        let (sender, worker_rx) = unbounded::<Worker>();
        let handles = (0..num)
            .map(|_| {
                let worker_rx_cloned = worker_rx.clone();
                thread::spawn(move || {
                    while let Ok(worker) = worker_rx_cloned.recv() {
                        // let unwind_safe_worker = AssertUnwindSafe(worker);
                        let _ = catch_unwind(AssertUnwindSafe(|| worker()));
                    }
                })
            })
            .collect::<Vec<_>>();

        Self { sender, handles }
    }

    pub fn spawn<'scope, F>(&self, f: F)
    where
        F: FnOnce() -> () + Send + 'scope,
    {
        let f = unsafe {
            std::mem::transmute::<
                Box<dyn FnOnce() -> () + Send + 'scope>,
                Box<dyn FnOnce() -> () + Send + 'static>,
            >(Box::new(f))
        };
        self.sender.send(f);
    }

    pub fn recv<'scope, F, R>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce() -> R + Send + 'scope,
        R: Send + 'static,
    {
        let (value_tx, value_rs) = oneshot();
        self.spawn(move || {
            let value = f();
            value_tx.send(value);
        });
        value_rs.recv()
    }

    pub fn join(self) {
        let Self { sender, handles } = self;
        drop(sender);
        for handle in handles {
            handle.join();
        }
    }
}

#[test]
fn test_thread_pool_recv() {
    let pool = ThreadPool::new(5);
    let mut data: Vec<_> = (0..100).collect();
    let result = data
        .iter_mut()
        .map(|i| pool.recv(move || *i * *i))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(result, (0..100).map(|i| i * i).collect::<Vec<_>>());
}

#[test]
fn test_thread_pool_join() {
    let pool = ThreadPool::new(5);
    let mut data: Vec<_> = (0..100).collect();
    for x in data.iter_mut() {
        pool.spawn(|| *x *= *x);
    }
    pool.join();
    assert_eq!(data, (0..100).map(|i| i * i).collect::<Vec<_>>());
}
