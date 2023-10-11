use bytes::BufMut;

use super::Block;
use super::{CHECKSUM_SIZE, COUNT_SIZE};
#[cfg(feature = "checksum")]
use crc32fast;

/// Builds a block.
pub struct BlockBuilder {
    cap: usize,
    data: Vec<u8>,
    offsets: Vec<u16>,
    #[cfg(feature = "checksum")]
    padding: u16,
    #[cfg(feature = "checksum")]
    hasher: crc32fast::Hasher,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        // alignment
        // assert!(block_size.count_ones() == 1 && block_size >= 512);

        Self {
            cap: block_size,
            data: vec![],
            offsets: vec![],
            #[cfg(feature = "checksum")]
            padding: 0,
            #[cfg(feature = "checksum")]
            hasher: crc32fast::Hasher::new(),
        }
    }

    // fn extend(&mut self, bytes: &[u8]) {
    //     #[cfg(feature = "checksum")]
    //     self.hasher.update(bytes);
    // }

    fn remaining(&self) -> isize {
        let meta_len = COUNT_SIZE + CHECKSUM_SIZE;
        let used = self.data.len() + self.offsets.len() * 2 + meta_len;

        self.cap as isize - used as isize
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let len = 2 + key.len() + 2 + value.len();
        let meta_len = COUNT_SIZE + CHECKSUM_SIZE;

        debug_assert!(self.remaining() >= 0);

        // TODO: better tests
        // assert!(2 + key.len() + 2 + value.len() + 2 + COUNT_SIZE + CHECKSUM_SIZE <= self.cap);

        if self.data.len() + len + meta_len > self.cap {
            // encoded size
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16_le(key.len() as u16);
        self.data.put_slice(key);
        self.data.put_u16_le(value.len() as u16);
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        let padding = self.remaining() as _;

        #[cfg(feature = "checksum")]
        {
            self.block
                .offsets
                .iter()
                .for_each(|off| self.hasher.update(&off.to_le_bytes()));

            self.hasher
                .update(&(self.block.offsets.len() as u16).to_le_bytes());
            self.block.sum = self.hasher.finalize();
        }

        Block {
            data: self.data,
            offsets: self.offsets,
            padding,
        }
    }

    pub fn size(&self) -> usize {
        self.cap - self.remaining() as usize
    }
}
