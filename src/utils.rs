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
