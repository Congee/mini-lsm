use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Bytes, BytesMut};

use super::{Block, BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::lsm_storage::BlockCache;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    builder: BlockBuilder,
    blocks: Vec<Block>,
    // Add other fields you need.
    block_size: usize,
    offset: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: vec![],
            builder: BlockBuilder::new(block_size),
            blocks: vec![],
            block_size,
            offset: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        while !self.builder.add(key, value) {
            let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let block = builder.build();

            self.meta.push(BlockMeta {
                offset: self.offset,
                first_key: Bytes::copy_from_slice(block.slice_at(0)),
            });
            self.offset += block.len();

            self.blocks.push(block);
        }
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        self.blocks.iter().fold(0, |acc, blk| acc + blk.len()) + self.builder.size()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut blocks = self.blocks;
        let mut block_metas = self.meta;
        if !self.builder.is_empty() {
            let block = self.builder.build();
            block_metas.push(BlockMeta {
                offset: self.offset,
                first_key: Bytes::copy_from_slice(block.slice_at(0)),
            });
            blocks.push(block);
        }

        let mut buf = blocks.iter().fold(BytesMut::new(), |mut acc, blk| {
            acc.extend_from_slice(&blk.encode());
            acc
        });
        let offset = buf.len();

        let mut vec = vec![];
        BlockMeta::encode_block_meta(&block_metas, &mut vec);
        buf.extend_from_slice(&vec);
        buf.extend_from_slice(&(offset as u32).to_le_bytes());

        Ok(SsTable {
            file: FileObject::create(path.as_ref(), buf.to_vec())?,
            block_metas,
            block_meta_offset: offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
