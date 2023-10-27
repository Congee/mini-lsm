use std::io::Write;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;

use anyhow::Result;
use bytes::Bytes;
use bytes::BytesMut;
use libc;

use crate::mem_table::MemTable;

// ioctl(file, BLKGETSIZE64, &file_size_in_bytes);
const ALIGNMENT_SIZE: usize = 4096;
const U16SZ: usize = std::mem::size_of::<u16>();

struct Wal {
    file: std::fs::File,
}

impl Wal {
    /// O_DIRECT | O_DSYNC is used for latency. Need batch/buffer for throughput
    pub fn create<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
            .open(&path)?;

        Ok(Self { file })
    }

    pub fn from<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)?;

        Ok(Self { file })
    }

    pub fn append(&mut self, key: &Bytes, value: &Bytes) -> Result<()> {
        let key_len = &(key.len() as u16).to_le_bytes();
        let val_len = &(value.len() as u16).to_le_bytes();
        let complement = ALIGNMENT_SIZE - (U16SZ * 2 + key.len() + value.len()) % ALIGNMENT_SIZE;

        let total = 4 + key.len() + value.len() + complement;
        let mut buf = Vec::with_capacity(total);

        // iovec still writes buffer by buffer which is align guaranteed
        buf.extend_from_slice(key_len.as_ref());
        buf.extend_from_slice(val_len.as_ref());
        buf.extend_from_slice(key.as_ref());
        buf.extend_from_slice(value.as_ref());
        buf.resize(total, 0);

        self.file.write_all(&buf)?;

        Ok(())
    }

    pub fn to_memtable(&self) -> Result<MemTable> {
        let tbl = MemTable::create();
        let mut buf = [0u8; ALIGNMENT_SIZE as usize];

        let file_len = self.file.metadata()?.len();
        assert_eq!(file_len % ALIGNMENT_SIZE as u64, 0);

        // read pair by pair
        enum Reading {
            Start,
            Cont,
        }

        // |_________________buf________________|
        // |head|_key_|_val_|
        // |head|_______________________key_____|val|
        // |head|______________________________________key|val|
        // |head|key|_________________val___________|
        let mut state = Reading::Start;
        let mut read = 0;
        let mut remaining = usize::MAX;
        let mut buffer = BytesMut::new();
        while read < file_len {
            self.file.read_exact_at(&mut buf, read)?;

            match state {
                Reading::Start => {
                    let header = 4usize;
                    let (key_len, val_len) = self.header_of(&buf);
                    let total = header + key_len + val_len;

                    if total <= ALIGNMENT_SIZE {
                        buffer.extend_from_slice(&buf[..total]);
                        let (key, value) = self.consume_buffer(&mut buffer);
                        tbl.put(key, value);
                        remaining = usize::MAX;
                        state = Reading::Start;

                    } else {
                        buffer.extend_from_slice(&buf);
                        remaining = total - ALIGNMENT_SIZE;
                        state = Reading::Cont;
                    }
                }
                Reading::Cont => {
                    let off = remaining.min(ALIGNMENT_SIZE);
                    buffer.extend_from_slice(&buf[..off]);
                    remaining -= off;

                    if remaining == 0 {
                        let (key, value) = self.consume_buffer(&mut buffer);
                        tbl.put(key, value);
                        state = Reading::Start;
                        remaining = usize::MAX;
                    } else {
                        state = Reading::Cont;
                    }
                }
            }

            read += ALIGNMENT_SIZE as u64;
        }

        Ok(tbl)
    }

    fn consume_buffer(&self, buffer: &mut BytesMut) -> (Bytes, Bytes) {
        let key_len = self.header_of(&buffer).0;
        let mut kv = buffer.split_off(4);
        let value = kv.split_off(key_len);
        let key = kv;

        buffer.clear();
        (key.freeze(), value.freeze())
    }

    fn header_of<T: AsRef<[u8]>>(&self, buf: &T) -> (usize, usize) {
        debug_assert!(buf.as_ref().len() >= 4);
        let key_len = u16::from_le_bytes(buf.as_ref()[..2].try_into().unwrap()) as usize;
        let val_len = u16::from_le_bytes(buf.as_ref()[2..4].try_into().unwrap()) as usize;
        (key_len, val_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn test_tiny() -> Result<()> {
        let dir = tempfile::tempdir_in(".")?;
        let path = dir.path().join("file");
        let mut wal = Wal::create(&path)?;
        wal.append(&Bytes::from("0"), &Bytes::from("0"))?;
        drop(wal);

        let wal = Wal::from(&path)?;
        let tbl = wal.to_memtable()?;
        assert_eq!(tbl.get(b"0"), Some(Bytes::from("0")));

        Ok(())
    }

    #[test]
    fn test_exceed_alignment() -> Result<()> {
        let dir = tempfile::tempdir_in(".")?;
        let path = dir.path().join("file");
        let mut wal = Wal::create(&path)?;
        let key = Bytes::from_static(b"1");
        let mut val = BytesMut::from_iter(b"2");
        val.put_bytes(b'a', ALIGNMENT_SIZE - 4 - 1 - 1);
        val.put_bytes(b'b', 1);
        let val = val.freeze();

        wal.append(&key, &val)?;
        drop(wal);

        let wal = Wal::from(&path)?;
        let tbl = wal.to_memtable()?;
        assert_eq!(tbl.get(&key), Some(val));

        Ok(())
    }
}
