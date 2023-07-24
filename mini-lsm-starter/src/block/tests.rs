use std::sync::Arc;

use super::builder::BlockBuilder;
use super::iterator::BlockIterator;
use super::*;

#[test]
fn test_block_build_single_key() {
    let mut builder = BlockBuilder::new(16);
    assert!(builder.add(b"233", b"233333"));
    builder.build();
}

#[test]
fn test_block_build_full() {
    let mut builder = BlockBuilder::new(16);
    assert!(builder.add(b"11", b"11"));
    assert!(!builder.add(b"22", b"22"));
    builder.build();
}

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    100
}

fn generate_block() -> Block {
    let mut builder = BlockBuilder::new(10000);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        assert!(builder.add(&key[..], &value[..]));
    }
    builder.build()
}

#[test]
fn test_block_build_all() {
    generate_block();
}

#[test]
fn test_block_encode() {
    let block = generate_block();
    block.encode();
}

#[test]
fn test_block_decode() {
    let block = generate_block();
    let encoded = block.encode();
    let decoded_block = {
        let data: &[u8] = &encoded;
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
    };
    assert_eq!(block.offsets, decoded_block.offsets);
    assert_eq!(block.data, decoded_block.data);
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

#[test]
fn test_block_iterator() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_first(block);
    // assert!(false);
    for _ in 0..5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key,
                key_of(i),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(&key_of(i)),
                as_bytes(key)
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.next();
        }
        iter.seek_to_first();
    }
}

#[test]
fn test_block_seek_key() {
    let block = Arc::new(generate_block());
    let mut iter = BlockIterator::create_and_seek_to_key(block, &key_of(0));
    for offset in 1..=5 {
        for i in 0..num_of_keys() {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key,
                key_of(i),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(&key_of(i)),
                as_bytes(key)
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.seek_to_key(&format!("key_{:03}", i * 5 + offset).into_bytes());
        }
        iter.seek_to_key(b"k");
    }
}
