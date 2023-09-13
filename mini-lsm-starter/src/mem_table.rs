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
    map: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        Self {
            map: Arc::new(SkipMap::<Bytes, Bytes>::new()),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let lower = lower.map(Bytes::copy_from_slice);
        let upper = upper.map(Bytes::copy_from_slice);
        // let curr = self
        //     .map
        //     .lower_bound(lower.as_ref())
        //     .map(|entry| (entry.key().clone(), entry.value().clone()));

        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            curr: None,
        }
        .build();
        let _ = iter.next();  // XXX: This is anti-pattern
        iter
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        self.map.iter().for_each(|entry| builder.add(entry.key(), entry.value()));

        Ok(())
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
    fn value(&self) -> &[u8] {
        &self
            .with_curr(|curr| curr.as_ref().map(|(_, value)| value))
            .unwrap()
    }

    fn key(&self) -> &[u8] {
        &self
            .with_curr(|curr| curr.as_ref().map(|(key, _)| key))
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
