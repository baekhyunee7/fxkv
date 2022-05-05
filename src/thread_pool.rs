use crossbeam::channel::{unbounded, Sender};
use crossbeam::sync::WaitGroup;
use gstuff::oneshot::oneshot;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::thread;
use std::thread::JoinHandle;

pub type Worker = Box<dyn FnOnce() -> () + Send + 'static>;

pub struct ThreadPool {
    pub sender: Option<Sender<Worker>>,
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

        Self {
            sender: Some(sender),
            handles,
        }
    }

    fn run<'scope, F>(&self, f: F)
    where
        F: FnOnce() -> () + Send + 'scope,
    {
        let f = unsafe {
            std::mem::transmute::<
                Box<dyn FnOnce() -> () + Send + 'scope>,
                Box<dyn FnOnce() -> () + Send + 'static>,
            >(Box::new(f))
        };
        let _ = self.sender.as_ref().unwrap().send(f);
    }

    pub fn recv<'scope, F, R>(&self, f: F) -> Result<R, String>
    where
        F: FnOnce() -> R + Send + 'scope,
        R: Send + 'static,
    {
        let (value_tx, value_rs) = oneshot();
        self.run(move || {
            let value = f();
            value_tx.send(value);
        });
        value_rs.recv()
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() -> () + Send + 'static,
    {
        let _ = self.sender.as_ref().unwrap().send(Box::new(f));
    }

    fn join(&mut self) {
        let sender = self.sender.take().unwrap();
        drop(sender);
        let handlers = std::mem::take(&mut self.handles);
        for handle in handlers {
            let _ = handle.join();
        }
    }

    pub fn scoped<F>(&self, f: F)
    where
        F: FnOnce(&Scoped) -> (),
    {
        let wg = WaitGroup::new();
        let scoped = Scoped {
            pool: self,
            wait_group: wg.clone(),
        };
        f(&scoped);
        drop(scoped);
        wg.wait();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.join()
    }
}

pub struct Scoped<'pool> {
    pub pool: &'pool ThreadPool,
    pub wait_group: WaitGroup,
}

impl<'pool> Scoped<'pool> {
    pub fn spawn<'scoped, F>(&self, f: F)
    where
        F: FnOnce() -> () + 'scoped + Send,
    {
        let wg = self.wait_group.clone();
        self.pool.run(move || {
            f();
            drop(wg);
        });
    }
}

#[cfg(test)]
mod test {
    use crate::thread_pool::ThreadPool;
    use std::sync::{Arc, Mutex};

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
        let data = Arc::new(Mutex::new(0));
        {
            let pool = ThreadPool::new(5);

            for _ in 0..100 {
                let cloned = data.clone();
                pool.spawn(move || {
                    let mut guard = cloned.lock().unwrap();
                    *guard += 1;
                });
            }
        }
        assert_eq!(*data.lock().unwrap(), 100);
    }

    #[test]
    fn test_thread_pool_scoped() {
        let pool = ThreadPool::new(5);
        let mut data: Vec<_> = (0..100).collect();
        pool.scoped(|scoped| {
            for i in data.iter_mut() {
                scoped.spawn(|| *i *= *i);
            }
        });
        assert_eq!(data, (0..100).map(|i| i * i).collect::<Vec<_>>());
    }
}
