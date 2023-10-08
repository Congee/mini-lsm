#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::RwLock;

use super::iterators::StorageIterator;
use crate::block::{Block, BlockIterator};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::MemTable;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 0,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    dir: std::path::PathBuf,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            dir: path.as_ref().into(),
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.__get(key).map(|opt| match opt {
            Some(v) if !v.is_empty() => Some(v),
            _ => None,
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn __get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(v) = self.inner.read().memtable.get(key) {
            return Ok(Some(v));
        }

        if let Some(v) = self
            .inner
            .read()
            .imm_memtables
            .iter()
            .rev()
            .map(|mem| mem.get(key))
            .filter(|x| x.is_some())
            .next()
            .flatten()
        {
            return Ok(Some(v));
        }

        // Search backwards on all sstables considering tombstones
        self.inner
            .read()
            .l0_sstables
            .iter()
            .rev()
            .map(|sstable| {
                sstable.__find_block_idx(key).ok().map(|idx| {
                    sstable.read_block_cached(idx).map(|block| {
                        let iter = BlockIterator::create_and_seek_to_key(block, key);
                        Bytes::copy_from_slice(iter.value())
                    })
                })
            })
            .filter(|x| x.is_some())
            .next()
            .flatten()
            .transpose()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        self.inner.write().memtable.put(key, value);

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.inner.write().memtable.put(_key, b"");

        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let mut builder = SsTableBuilder::new(4096);
        let memtable = self.inner.read().memtable.clone();
        self.inner.read().memtable.flush(&mut builder)?;

        let filename = format!("{}.sst", self.inner.read().next_sst_id);
        let path = self.dir.join(filename);
        let sstable = builder.build(4096, Some(Arc::new(moka::sync::Cache::new(128))), &path)?;

        let guard = self.inner.write();
        let mut snapshot = guard.as_ref().clone();
        let memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::create()));
        snapshot.imm_memtables.push(memtable);
        snapshot.l0_sstables.push(Arc::new(sstable));
        snapshot.next_sst_id += 1;

        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let guard = self.inner.read();
        let mut mem_iters = vec![Box::new(guard.memtable.scan(_lower, _upper))];
        mem_iters.extend(
            guard
                .imm_memtables
                .iter()
                .map(|tbl| Box::new(tbl.scan(_lower, _upper))),
        );

        let sst_iters: Result<Vec<_>> = guard
            .l0_sstables
            .iter()
            .map(|sst| SsTableIterator::by_range(sst.clone(), _lower, _upper).map(Box::new))
            .into_iter()
            .collect();

        let mut two = TwoMergeIterator::create(
            MergeIterator::create(mem_iters),
            MergeIterator::create(sst_iters?),
        )?;

        // XXX: skip to first valid
        while two.is_valid() && two.value().is_empty() {
            two.next()?;
        }

        Ok(FusedIterator::new(LsmIterator::new(two)))
    }
}
