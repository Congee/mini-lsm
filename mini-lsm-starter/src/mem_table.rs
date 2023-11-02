use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::table::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    // needs interior mutability
    map: Arc<SkipMap<Bytes, Bytes>>,
    size: std::sync::atomic::AtomicUsize,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        Self {
            map: Arc::new(SkipMap::<Bytes, Bytes>::new()),
            size: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    pub fn size(&self) -> usize {
        self.size.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: Bytes, value: Bytes) {
        self.size
            .fetch_add(key.len() + value.len(), std::sync::atomic::Ordering::SeqCst);
        self.map.insert(key, value);
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let lower = lower.map(Bytes::copy_from_slice);
        let upper = upper.map(Bytes::copy_from_slice);

        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            curr: None,
        }
        .build();
        let _ = iter.next(); // XXX: This is anti-pattern
        iter
    }

    /// Flush the mem-table to SSTable.
    pub fn to_sst(&self, block_size: usize) -> SsTableBuilder {
        let mut builder = SsTableBuilder::new(block_size);
        self.map
            .iter()
            .for_each(|entry| builder.add(entry.key(), entry.value()));
        builder
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    curr: Option<(Bytes, Bytes)>,
}

impl StorageIterator for MemTableIterator {
    fn value(&self) -> &Bytes {
        self.with_curr(|curr| curr.as_ref().map(|(_, value)| value))
            .unwrap()
    }

    fn key(&self) -> &Bytes {
        self.with_curr(|curr| curr.as_ref().map(|(key, _)| key))
            .unwrap()
    }

    fn is_valid(&self) -> bool {
        self.with_curr(|curr| curr.is_some())
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            *fields.curr = fields
                .iter
                .next()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests;
