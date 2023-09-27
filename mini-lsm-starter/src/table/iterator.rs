use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::SsTable;
use crate::block::BlockIterator;
use crate::iterators::StorageIterator;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_idx: usize,
    iter: BlockIterator,
    lower: Bound<Bytes>,
    upper: Bound<Bytes>,
    in_bounds: bool,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_idx = 0;
        let block = table.read_block(blk_idx)?;
        let iter = BlockIterator::create_and_seek_to_first(block);

        Ok(Self {
            table,
            blk_idx,
            iter,
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            in_bounds: true,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        let block = self.table.read_block(self.blk_idx)?;
        self.iter = BlockIterator::create_and_seek_to_first(block);

        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let blk_idx = std::cmp::min(table.find_block_idx(key), table.num_of_blocks() - 1);
        let block = table.read_block(blk_idx)?;
        let iter = BlockIterator::create_and_seek_to_key(block.clone(), key);

        Ok(Self {
            table,
            blk_idx,
            iter,
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            in_bounds: true,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        self.iter = Self::create_and_seek_to_key(self.table.clone(), key)?.iter;

        Ok(())
    }

    pub fn by_range(table: Arc<SsTable>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<Self> {
        let mut this = match lower {
            Bound::Included(lo) => Self::create_and_seek_to_key(table, lo)?,
            Bound::Excluded(lo) => {
                let mut this = Self::create_and_seek_to_key(table, lo)?;
                if this.iter.is_valid() && this.iter.key() == lo {
                    this.iter.next();
                }
                this
            }
            Bound::Unbounded => Self::create_and_seek_to_first(table)?,
        };
        this.upper = upper.map(Bytes::copy_from_slice);
        Ok(this)
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.in_bounds && self.iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.iter.next();

        if self.iter.is_valid() {
            match &self.upper {
                Bound::Included(hi) if self.key() > hi => self.in_bounds = false,
                Bound::Excluded(hi) if self.key() >= hi => self.in_bounds = false,
                _ => {},
            };
            return Ok(());
        }

        self.blk_idx += 1;

        if self.blk_idx == self.table.num_of_blocks() {
            self.in_bounds = false;

            return Ok(()); // TODO: ??? return Err(anyhow!("iterator reached the end"));
        }

        let block = self.table.read_block(self.blk_idx)?;
        self.iter = BlockIterator::create_and_seek_to_first(block);

        Ok(())
    }
}
