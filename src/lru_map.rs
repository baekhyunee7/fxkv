use arrayvec::ArrayVec;
use std::collections::HashMap;
use std::hash::Hash;
use std::process::id;
use std::rc::Rc;

pub struct LruMap<K, V, const N: usize> {
    pub cache: ArrayVec<Entry<K, V>, N>,
    pub head: usize,
    pub tail: usize,
    pub indexes: HashMap<Rc<K>, usize>,
}

pub struct Entry<K, V> {
    pub key: Rc<K>,
    pub value: V,
    pub pre: usize,
    pub next: usize,
}

impl<K, V, const N: usize> LruMap<K, V, N>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            cache: ArrayVec::new(),
            head: 0,
            tail: 0,
            indexes: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let key = Rc::new(key);
        let entry = Entry {
            key: key.clone(),
            value,
            pre: 0,
            next: 0,
        };
        if self.cache.len() < self.cache.capacity() {
            self.cache.push(entry);
            let idx = self.cache.len() - 1;
            self.push_front(idx);
            self.indexes.insert(key.clone(), idx);
        } else {
            let old = self.pop_back();
            self.indexes.remove(&self.cache[old].key);
            std::mem::replace(&mut self.cache[old], entry);
            self.indexes.insert(key, old);
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(idx) = self.indexes.get(key) {
            let idx = *idx;
            self.remove(idx);
            self.push_front(idx);
            Some(&self.cache[idx].value)
        } else {
            None
        }
    }

    fn push_front(&mut self, i: usize) {
        self.cache[i].next = self.head;
        self.cache[self.head].pre = i;
        self.head = i;
    }

    fn pop_back(&mut self) -> usize {
        let tail = self.tail;
        self.tail = self.cache[self.tail].pre;
        tail
    }

    fn remove(&mut self, i: usize) {
        let pre = self.cache[i].pre;
        let next = self.cache[i].next;
        if i == self.head {
            self.head = next;
        } else if i == self.tail {
            self.tail = pre;
        } else {
            self.cache[pre].next = next;
            self.cache[next].pre = pre;
        }
    }
}

#[cfg(test)]
mod test {
    use crate::lru_map::LruMap;

    #[test]
    fn test_lru() {
        let mut lru = LruMap::<_, _, 3>::new();
        lru.insert("key1", 1);
        lru.insert("key2", 2);
        assert_eq!(lru.get(&"key1"), Some(&1));
        assert_eq!(lru.get(&"key2"), Some(&2));
        lru.insert("key3", 3);
        lru.insert("key4", 4);
        assert!(lru.get(&"key1").is_none());
        assert_eq!(lru.get(&"key2"), Some(&2));
        assert_eq!(lru.get(&"key3"), Some(&3));
        assert_eq!(lru.get(&"key4"), Some(&4));
    }
}
