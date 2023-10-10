mod builder;
mod iterator;

use std::cmp::max;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    ///
    /// | offset | key len | first_key |
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut bytes = BytesMut::new();
        for meta in block_meta {
            bytes.put_u32_le(meta.offset as _);
            bytes.put_u16_le(meta.first_key.len() as _);
            bytes.extend_from_slice(&meta.first_key);
        }
        buf.extend_from_slice(&bytes);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut buf = buf;
        let mut vec: Vec<BlockMeta> = vec![];
        while buf.has_remaining() {
            let offset = buf.get_u32_le() as usize;
            let key_len = buf.get_u16_le() as usize;
            let first_key = buf.copy_to_bytes(key_len);

            vec.push(Self { offset, first_key })
        }

        vec
    }
}

/// A file object.
pub struct FileObject {
    size: u64,
    file: std::fs::File,
}

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len as _];
        self.file.read_exact_at(buf.as_mut(), offset)?;
        Ok(buf)
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.write_all(&data)?;
        file.flush()?;

        Ok(Self {
            size: data.len() as _,
            file,
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let size = file.metadata()?.len();

        Ok(Self { size, file })
    }
}

/// -------------------------------------------------------------------------------------------------------
/// |              Data Block             |             Meta Block              |          Extra          |
/// -------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Meta Block #1 | ... | Meta Block #N | Meta Block Offset (u32) |
/// -------------------------------------------------------------------------------------------------------
pub struct SsTable {
    id: usize,
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: usize,

    cache: Option<Arc<BlockCache>>,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, Some(Arc::new(moka::sync::Cache::new(128))), file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let tail = file.read(file.size() - 4, 4)?;
        let start = u32::from_le_bytes(tail.as_slice().try_into().unwrap()) as u64;
        let buf = file.read(start, file.size() - 4 - start)?;

        Ok(Self {
            id,
            file,
            block_metas: BlockMeta::decode_block_meta(buf.as_slice()),
            block_meta_offset: start as usize,
            cache: block_cache,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let lo = self.block_metas[block_idx].offset as u64;
        let hi = match self.block_metas.get(block_idx + 1) {
            Some(&BlockMeta { offset, .. }) => offset,
            None => self.block_meta_offset,
        } as u64;

        Ok(Arc::new(Block::decode(&self.file.read(lo, hi - lo)?)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match &self.cache {
            Some(cache) => cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|err| anyhow::anyhow!(err)),
            _ => self.read_block(block_idx),
        }
    }

    pub fn __find_block_idx(&self, key: &[u8]) -> Result<usize, usize> {
        self.block_metas
            .binary_search_by(|meta| meta.first_key.as_ref().cmp(key))
            .map_err(|insert| {
                if insert == self.num_of_blocks() {
                    return max(0, insert as isize - 1) as usize;
                }

                if self.block_metas[insert].first_key.as_ref() > key {
                    let last = self
                        .read_block_cached(max(0, insert as isize - 1) as _)
                        .ok()
                        .map(|x| x.last() >= Some(key));

                    if last == Some(true) {
                        return max(0, insert as isize - 1) as usize;
                    } else {
                        return insert;
                    }
                }
                return insert;
            })
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.__find_block_idx(key)
            .unwrap_or_else(std::convert::identity)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

pub fn is_true(x: bool) -> bool {
    x
}

#[cfg(test)]
mod tests;
