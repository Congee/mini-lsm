use anyhow::Result;

use super::StorageIterator;
use bytes::Bytes;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    // TODO: static dispatch
    key: Bytes,
    value: Bytes,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut this = Self {
            a,
            b,
            key: Bytes::new(),
            value: Bytes::new(),
        };

        this.next()?;

        Ok(this)
    }

    fn copy_from_a(&mut self) {
        if self.a.is_valid() {
            self.key = self.a.key().clone();
            self.value = self.a.value().clone();
        }
    }

    fn copy_from_b(&mut self) {
        if self.b.is_valid() {
            self.key = self.b.key().clone();
            self.value = self.b.value().clone();
        }
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn value(&self) -> &Bytes {
        &self.value
    }

    fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() {
            match self.a.key().cmp(&self.b.key()) {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                    self.copy_from_a();
                    self.a.next()?;
                    self.b.next()?;
                }
                _ => {
                    self.copy_from_b();
                    self.b.next()?;
                }
            }
        } else if self.a.is_valid() {
            self.copy_from_a();
            self.a.next()?;
        } else if self.b.is_valid() {
            self.copy_from_b();
            self.b.next()?;
        } else {
            self.key = Bytes::new();
        }

        Ok(())
    }
}
