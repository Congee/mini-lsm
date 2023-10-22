pub mod merge_iterator;
pub mod two_merge_iterator;

use bytes::Bytes;

pub trait StorageIterator {
    /// Get the current value.
    fn value(&self) -> &Bytes;

    /// Get the current key.
    fn key(&self) -> &Bytes;

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position.
    fn next(&mut self) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests;
