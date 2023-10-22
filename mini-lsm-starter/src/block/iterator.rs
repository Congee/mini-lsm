use std::sync::Arc;

use bytes::Bytes;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: Bytes,
    /// The corresponding value, can be empty
    value: Bytes,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

type Entry = (Bytes, Bytes);

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Bytes::new(),
            value: Bytes::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        // TODO: self.block.offsets.first() > Some(0)?
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) -> Entry {
        if self.block.offsets.len() == self.idx + 1 {
            self.key.clear();
            self.idx = 0;
        } else {
            self.seek_to(self.idx + 1);
        }

        (self.key.clone(), self.value.clone())
    }

    fn seek_to(&mut self, idx: usize) {
        self.idx = idx;
        let pos = self.block.offsets[self.idx] as usize;
        self.key = Bytes::copy_from_slice(self.block.slice_at(pos));
        self.value = Bytes::copy_from_slice(self.block.slice_at(pos + 2 + self.key.len()));
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    /// similar to std::lower_bound
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut lo: isize = 0;
        let mut hi = self.block.offsets.len() as isize - 1;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;

            let curr = self.block.slice_at(self.block.offsets[mid as usize] as _);
            match curr.cmp(key) {
                std::cmp::Ordering::Less => {
                    lo = mid + 1;
                }
                std::cmp::Ordering::Equal => return self.seek_to(mid as _),
                std::cmp::Ordering::Greater => {
                    self.idx = mid as _;
                    hi = mid;
                }
            }
        }

        if self.block.slice_at(self.block.offsets[lo as usize] as _) >= key {
            self.seek_to(lo as _)
        } else {
            self.key.clear()
        }
    }
}
