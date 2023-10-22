use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;
use bytes::Bytes;

use super::StorageIterator;

pub struct IterWrapper<I: StorageIterator> {
    pub idx: usize,
    pub inner_iter: Box<I>,
}

impl<I: StorageIterator> PartialEq for IterWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for IterWrapper<I> {}

impl<I: StorageIterator> PartialOrd for IterWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.inner_iter.key().cmp(&other.inner_iter.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.idx.partial_cmp(&other.idx),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for IterWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<IterWrapper<I>>,
    current: Option<IterWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap: BinaryHeap<IterWrapper<I>> =
            BinaryHeap::from_iter(iters.into_iter().filter(|i| i.is_valid()).enumerate().map(
                |(idx, iter)| IterWrapper::<I> {
                    idx,
                    inner_iter: iter,
                },
            ));
        let current = heap.pop();

        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &Bytes {
        self.current.as_ref().unwrap().inner_iter.key()
    }

    fn value(&self) -> &Bytes {
        self.current.as_ref().unwrap().inner_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().map(|x| x.inner_iter.is_valid()) == Some(true)
    }

    fn next(&mut self) -> Result<()> {
        while self.is_valid()
            && self.iters.peek().map(|x| x.inner_iter.key())
                == self.current.as_ref().map(|x| x.inner_iter.key())
        {
            // NOTE: Avoid calling PeekMut::drop
            let mut opt = self.iters.pop().unwrap();
            opt.inner_iter.next()?;
            if opt.inner_iter.is_valid() {
                self.iters.push(opt);
            }
        }

        self.current.as_mut().unwrap().inner_iter.next()?;

        if self.is_valid() {
            self.iters.push(self.current.take().unwrap());
        }

        self.current = self.iters.pop();

        Ok(())
    }
}
