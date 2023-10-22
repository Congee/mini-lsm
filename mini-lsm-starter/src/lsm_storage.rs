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
use crate::table::{SsTable, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

const fn validate_block_size(size: usize) -> usize {
    // aligned to the power of 2
    if size.count_ones() != 1 {
        panic!("not to the power of 2");
    }

    if size < 4096 {
        panic!("size is too small");
    }

    size
}

static MIN_NUM_SST_FILES_TO_COMPACT: usize = 2;
static BLOCK_SIZE: usize = validate_block_size(4 * 1024);

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
    next_sst_id: usize, // TODO:
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

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(v) = self.memtable.get(key) {
            return Ok(Some(v));
        }

        if let Some(v) = self
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
        self.l0_sstables
            .iter()
            .rev()
            .map(|sstable| {
                sstable.__find_block_idx(key).ok().map(|idx| {
                    sstable.read_block_cached(idx).map(|block| {
                        let iter = BlockIterator::create_and_seek_to_key(block, key);
                        iter.value().clone()
                    })
                })
            })
            .filter(|x| x.is_some())
            .next()
            .flatten()
            .transpose()
    }

    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let mut mem_iters = vec![Box::new(self.memtable.scan(_lower, _upper))];
        mem_iters.extend(
            self.imm_memtables
                .iter()
                .map(|tbl| Box::new(tbl.scan(_lower, _upper))),
        );

        let sst_iters: Result<Vec<_>> = self
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

    pub fn archive_mem_table(&mut self) {
        self.imm_memtables.push(std::mem::replace(
            &mut self.memtable,
            Arc::new(MemTable::create()),
        ));
    }
}

/// The storage interface of the LSM tree.
#[derive(Clone)]
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    dir: std::path::PathBuf,
    cache: Arc<BlockCache>,
    sync_tx: flume::Sender<Option<()>>,
    sync_rx: flume::Receiver<Option<()>>,
}

impl Drop for LsmStorage {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let (tx, rx) = flume::unbounded();
        let lsm = Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            dir: path.as_ref().into(),
            cache: Arc::new(BlockCache::new(1 << 20)),
            sync_tx: tx,
            sync_rx: rx,
        };

        let this = lsm.clone();
        std::thread::spawn(move || {
            this.loop_compaction().unwrap();
        });

        Ok(lsm)
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.read().get(key).map(|opt| match opt {
            Some(v) if !v.is_empty() => Some(v),
            _ => None,
        })
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        let inner = self.inner.write().as_ref().clone();

        let mem = inner.memtable.clone();
        mem.put(key, value);

        if mem.size() > 1000000 {
            // TODO:
            self.sync_tx.send(Some(()))?;
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.inner
            .write()
            .as_ref()
            .clone()
            .memtable
            .put(Bytes::copy_from_slice(_key), Bytes::new());

        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    // XXX: no contention for self.sync()
    pub fn sync(&self) -> Result<()> {
        let guard = self.inner.write();
        let mut inner = guard.as_ref().clone();
        let next_sst_id = inner.next_sst_id;
        let path = self.path_of_sst(next_sst_id);

        inner.archive_mem_table();

        let builder = inner.imm_memtables.last().unwrap().to_sst(BLOCK_SIZE);
        let sstable = builder.export(next_sst_id, Some(self.cache.clone()), &path)?;

        inner.l0_sstables.push(Arc::new(sstable));
        inner.next_sst_id += 1;

        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.read().scan(_lower, _upper)
    }

    fn loop_compaction(&self) -> Result<()> {
        for msg in self.sync_rx.iter() {
            if msg.is_none() {
                return Ok(());
            }

            self.sync()?;

            let guard = self.inner.write();

            if guard.l0_sstables.len() == MIN_NUM_SST_FILES_TO_COMPACT {
                self.compact(0)?;
            }

            for level in guard
                .levels
                .iter()
                .filter(|vec| vec.len() == MIN_NUM_SST_FILES_TO_COMPACT)
                .enumerate()
                .map(|(idx, _)| idx + 1)
            {
                self.compact(level)?;
            }
        }

        unreachable!();
    }

    /// Optimizing Space Amplification in RocksDB
    /// https://www.cidrdb.org/cidr2017/papers/p82-dong-cidr17.pdf
    pub fn compact(&self, level: usize) -> Result<()> {
        // TODO: how long should I hold this lock?
        let guard = self.inner.read();

        let ssts = match level {
            0 => &guard.l0_sstables,
            x => &guard.levels[x],
        };

        let mut iters = ssts
            .iter()
            .map(|sst| SsTableIterator::create_and_seek_to_first(sst.clone()).map(Box::new))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        if let Some(next_level_sst) = guard.levels[level].get(0) {
            iters.push(
                SsTableIterator::create_and_seek_to_first(next_level_sst.clone()).map(Box::new)?,
            )
        }

        drop(guard);

        // TODO: do not load everything into memory. stream it to disk by batch
        let mut iter = MergeIterator::create(iters);
        let mem = MemTable::create();
        while iter.is_valid() {
            if !iter.value().is_empty() {
                mem.put(iter.key().clone(), iter.value().clone())
            };
            iter.next()?;
        }

        let builder = mem.to_sst(BLOCK_SIZE);
        let next_sst_id = self.inner.read().next_sst_id;
        let path = self.path_of_sst(next_sst_id);
        let sstable = builder.export(next_sst_id, Some(self.cache.clone()), &path)?;
        // delete all input sstables and replace them with the new sstable in the next level

        let mut inner = self.inner.write().as_ref().clone();
        match level {
            0 => inner.l0_sstables.clear(),
            x => inner.levels[x].clear(),
        };

        inner.levels[level].push(Arc::new(sstable));

        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        self.sync_tx.send(None).map_err(|x| anyhow::anyhow!(x))
    }

    fn path_of_sst(&self, sst_id: usize) -> std::path::PathBuf {
        self.dir.join(format!("{}.sst", sst_id))
    }
}
