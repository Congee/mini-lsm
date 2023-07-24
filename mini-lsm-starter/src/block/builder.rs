use super::Block;
use crc32fast;

/// Builds a block.
pub struct BlockBuilder {
    block: Block,
    offset: usize,
    cap: usize,
    hasher: crc32fast::Hasher,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        // alignment
        // assert!(block_size.count_ones() == 1 && block_size >= 512);

        let block = Block {
            data: vec![],
            padding: vec![],
            offsets: vec![],
            sum: 0,
        };

        Self {
            block,
            offset: 0,
            cap: block_size,
            hasher: crc32fast::Hasher::new(),
        }
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.block.data.extend_from_slice(bytes);
        self.hasher.update(bytes);
    }

    fn remaining(&self) -> usize {
        self.cap - self.block.data.len() - self.block.offsets.len() * 2 - 2 - 4
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let len = 2 + key.len() + 2 + value.len();
        let meta_len = 2;

        if self.remaining() > 2 + 2 {}

        if self.offset + len + meta_len > self.cap {
            // encoded size
            return false;
        }

        self.block.offsets.push(self.offset as u16);
        self.offset += len;

        self.extend(&(key.len() as u16).to_le_bytes());
        self.extend(key);
        self.extend(&(value.len() as u16).to_le_bytes());
        self.extend(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offset == 0
    }

    /// Finalize the block.
    pub fn build(mut self) -> Block {
        self.block.padding = vec![0; self.remaining()];

        self.block
            .offsets
            .iter()
            .for_each(|off| self.hasher.update(&off.to_le_bytes()));
        self.hasher
            .update(&(self.block.offsets.len() as u16).to_le_bytes());
        self.block.sum = self.hasher.finalize();

        self.block
    }
}
