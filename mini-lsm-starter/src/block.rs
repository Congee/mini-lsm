mod builder;
mod iterator;

pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// ----------------------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |  CheckSum |
/// ----------------------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |  crc32    |
/// ----------------------------------------------------------------------------------------------------------------
pub struct Block {
    data: Vec<u8>,
    padding: u16,
    offsets: Vec<u16>,
    sum: u32,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::from(self.data.as_slice());
        self.offsets
            .iter()
            .for_each(|offset| bytes.put_u16_le(*offset));
        bytes.extend_from_slice(&vec![0; self.padding as _]);
        bytes.put_u16_le(self.offsets.len() as _);
        bytes.put_u32_le(self.sum);
        bytes.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut hasher = crc32fast::Hasher::new();

        let sum = u32::from_le_bytes(data[data.len() - 4..data.len()].try_into().unwrap());
        let count =
            u16::from_le_bytes(data[data.len() - 4 - 2..data.len() - 4].try_into().unwrap());

        let mut raw = vec![];
        for _ in 0..count {
            let key_len = u16::from_le_bytes(data[raw.len()..raw.len() + 2].try_into().unwrap());
            raw.extend_from_slice(&data[raw.len()..raw.len() + 2 + key_len as usize]);
            let val_len = u16::from_le_bytes(data[raw.len()..raw.len() + 2].try_into().unwrap());
            raw.extend_from_slice(&data[raw.len()..raw.len() + 2 + val_len as usize]);
        }
        // let raw = data[..data.len() - 4 - 2 - count as usize * 2].to_vec();

        // NOTE: don't use Vec::<_>::from_raw_parts because of alignment 1 -> 2
        let off = &data[data.len() - 4 - 2 - count as usize * 2..data.len() - 4 - 2];
        let offsets = off
            .chunks(2)
            .map(|chk| u16::from_le_bytes(chk.try_into().unwrap()))
            .collect::<Vec<u16>>();
        // let offsets =
        //     unsafe { std::slice::from_raw_parts(off.as_ptr() as *const u16, count as _).to_vec() };

        hasher.update(&raw);
        hasher.update(off);
        hasher.update(&count.to_le_bytes());

        // TODO: return a Result on corruption
        assert!(sum == hasher.finalize());

        Block {
            data: raw,
            padding: 0,
            offsets,
            sum,
        }
    }

    pub fn slice_at(&self, pos: usize) -> &[u8] {
        let key_len = u16::from_le_bytes(self.data[pos..pos + 2].try_into().unwrap());
        &self.data[pos + 2..pos + 2 + key_len as usize]
    }
}

#[cfg(test)]
mod tests;
