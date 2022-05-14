use crate::{Error, Result};
use gstuff::oneshot::{oneshot, Sender};
use spin::mutex::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Lock {
    pub locked: AtomicBool,
    pub pendings: Mutex<Vec<Sender<()>>>,
}

impl Lock {
    pub fn new() -> Self {
        Self {
            locked: AtomicBool::new(false),
            pendings: Mutex::new(vec![]),
        }
    }
    pub fn lock(&self) -> Result<()> {
        loop {
            if self
                .locked
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            let mut guard = self.pendings.lock();
            if !self
                .locked
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            let (tx, rx) = oneshot();
            guard.push(tx);
            drop(guard);
            return Ok(rx.recv().map_err(Error::Unknown)?);
        }
    }

    pub fn unlock(&self) {
        let mut guard = self.pendings.lock();
        if self
            .locked
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::Acquire)
            .is_ok()
        {
            for pending in guard.drain(..) {
                pending.send(())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::lock::Lock;
    use crossbeam::sync::WaitGroup;

    use std::sync::Arc;
    use std::thread;

    pub struct SendIntPtr {
        pub ptr: *mut i32,
    }

    impl SendIntPtr {
        pub fn add(&self) {
            unsafe {
                *self.ptr += 1;
            }
        }
    }

    unsafe impl Send for SendIntPtr {}

    impl Clone for SendIntPtr {
        fn clone(&self) -> Self {
            SendIntPtr { ptr: self.ptr }
        }

        fn clone_from(&mut self, source: &Self) {
            self.ptr = source.ptr;
        }
    }

    #[test]
    fn test_mutex() {
        let mut num = 0;
        let send_ptr = SendIntPtr { ptr: &mut num };
        let lock = Arc::new(Lock::new());
        let wg = WaitGroup::new();
        for _ in 0..100 {
            let lock_cloned = lock.clone();
            let wg_cloned = wg.clone();
            let ptr = send_ptr.clone();
            thread::spawn(move || {
                lock_cloned.lock().unwrap();
                ptr.add();
                lock_cloned.unlock();
                drop(wg_cloned);
            });
        }
        wg.wait();
        assert_eq!(num, 100);
    }
}
