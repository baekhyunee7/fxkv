pub struct First<T> {
    pub first: T,
    pub next: T,
    pub inited: bool,
}

impl<T> First<T>
where
    T: Clone,
{
    pub fn new(first: T, next: T) -> Self {
        Self {
            inited: false,
            first,
            next,
        }
    }

    pub fn get(&mut self) -> T {
        if !self.inited {
            self.inited = true;
            self.first.clone()
        } else {
            self.next.clone()
        }
    }

    pub fn first(&self) -> bool {
        !self.inited
    }
}

pub struct Windows {
    pub head: usize,
    pub bitmap: Vec<u8>,
}

const BYTE_SIZE: usize = 8;

impl Windows {
    pub fn start_with(head: usize) -> Self {
        Self {
            head,
            bitmap: vec![0_u8; 16],
        }
    }

    pub fn put(&mut self, i: usize) {
        assert!(i >= self.head);
        let diff = i - self.head;
        let idx = diff / BYTE_SIZE;
        let bit = diff % BYTE_SIZE;
        if idx >= self.bitmap.len() {
            self.bitmap.resize(idx + 1, 0);
        }
        let old = self.bitmap[idx];
        self.bitmap[idx] = old | 1_u8 << (BYTE_SIZE - bit - 1);
    }

    pub fn completed(&mut self) -> bool {
        if let Some((idx, data)) = self
            .bitmap
            .iter()
            .enumerate()
            .skip_while(|(idx, x)| **x == 0xff)
            .nth(0)
        {
            self.head += idx * BYTE_SIZE;
            let n: i32 = match data {
                0x00 => 0,
                0x80 => 1,
                0xC0 => 2,
                0xE0 => 3,
                0xF0 => 4,
                0xF8 => 5,
                0xFC => 6,
                0xFE => 7,
                _ => -1,
            };
            self.bitmap.drain(..idx);
            !(n < 0
                || (n == 0 && idx == 0)
                || (idx + 1 < self.bitmap.len()
                    && (idx + 1..self.bitmap.len())
                        .map(|i| self.bitmap[i])
                        .any(|x| x != 0)))
        } else {
            self.head += self.bitmap.len() * BYTE_SIZE;
            self.bitmap = vec![0_u8; 16];
            true
        }
    }
}

#[test]
fn test_windows() {
    let mut windows = Windows::start_with(100);
    for i in 100..107 {
        windows.put(i);
        assert!(windows.completed());
        assert_eq!(windows.head, 100);
    }
    windows.put(107);
    assert!(windows.completed());
    assert_eq!(windows.head, 108);

    let mut windows = Windows::start_with(100);
    for i in (100..500).rev() {
        windows.put(i);
        assert_eq!(windows.completed(), i == 100);
    }
    assert_eq!(windows.head, 500);
}
