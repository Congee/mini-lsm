use anyhow::Result;
use bytes::Bytes;

use super::StorageIterator;

pub mod merge_iterator_test;
pub mod two_merge_iterator_test;

#[derive(Clone)]
pub struct MockIterator {
    pub data: Vec<(Bytes, Bytes)>,
    pub index: usize,
}

impl MockIterator {
    pub fn new(data: Vec<(Bytes, Bytes)>) -> Self {
        Self { data, index: 0 }
    }
}

impl StorageIterator for MockIterator {
    fn next(&mut self) -> Result<()> {
        if self.index < self.data.len() {
            self.index += 1;
        }
        Ok(())
    }

    fn key(&self) -> &Bytes {
        &self.data[self.index].0
    }

    fn value(&self) -> &Bytes {
        &self.data[self.index].1
    }

    fn is_valid(&self) -> bool {
        self.index < self.data.len()
    }
}
