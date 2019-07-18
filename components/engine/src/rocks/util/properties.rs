// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::ops::{Deref, DerefMut};
use std::u64;

use crate::rocks::{
    DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory, TitanBlobIndex,
    UserCollectedProperties,
};
use tikv_util::codec::number::{self, NumberEncoder};
use tikv_util::codec::{Error, Result};

const PROP_TOTAL_SIZE: &str = "tikv.total_size";
const PROP_SIZE_INDEX: &str = "tikv.size_index";
const PROP_RANGE_INDEX: &str = "tikv.range_index";
pub const DEFAULT_PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;
pub const DEFAULT_PROP_KEYS_INDEX_DISTANCE: u64 = 40 * 1024;

fn get_entry_size(value: &[u8], entry_type: DBEntryType) -> std::result::Result<u64, ()> {
    match entry_type {
        DBEntryType::Put => Ok(value.len() as u64),
        DBEntryType::BlobIndex => match TitanBlobIndex::decode(value) {
            Ok(index) => Ok(index.blob_size + value.len() as u64),
            Err(_) => Err(()),
        },
        _ => Err(()),
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexHandle {
    pub size: u64,   // The size of the stored block
    pub offset: u64, // The offset of the block in the file
}

#[derive(Debug, Default)]
pub struct IndexHandles(BTreeMap<Vec<u8>, IndexHandle>);

impl Deref for IndexHandles {
    type Target = BTreeMap<Vec<u8>, IndexHandle>;
    fn deref(&self) -> &BTreeMap<Vec<u8>, IndexHandle> {
        &self.0
    }
}

impl DerefMut for IndexHandles {
    fn deref_mut(&mut self) -> &mut BTreeMap<Vec<u8>, IndexHandle> {
        &mut self.0
    }
}

impl IndexHandles {
    // Format: | klen | k | v.size | v.offset |
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);
        for (k, v) in &self.0 {
            buf.encode_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.encode_u64(v.size).unwrap();
            buf.encode_u64(v.offset).unwrap();
        }
        buf
    }

    #[allow(dead_code)]
    fn decode(mut buf: &[u8]) -> Result<IndexHandles> {
        let mut res = BTreeMap::new();
        while !buf.is_empty() {
            let klen = number::decode_u64(&mut buf)?;
            let mut k = vec![0; klen as usize];
            buf.read_exact(&mut k)?;
            let mut v = IndexHandle::default();
            v.size = number::decode_u64(&mut buf)?;
            v.offset = number::decode_u64(&mut buf)?;
            res.insert(k, v);
        }
        Ok(IndexHandles(res))
    }
}

#[derive(Default)]
pub struct RowsProperties {
    pub total_rows: u64,
    pub index_handles: IndexHandles,
}

#[derive(Debug, Default)]
pub struct SizeProperties {
    pub total_size: u64,
    pub index_handles: IndexHandles,
}

impl SizeProperties {
    pub fn encode(&self) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_TOTAL_SIZE, self.total_size);
        props.encode_handles(PROP_SIZE_INDEX, &self.index_handles);
        props
    }
}

pub struct SizePropertiesCollector {
    props: SizeProperties,
    last_key: Vec<u8>,
    index_handle: IndexHandle,
}

impl SizePropertiesCollector {
    fn new() -> SizePropertiesCollector {
        SizePropertiesCollector {
            props: SizeProperties::default(),
            last_key: Vec::new(),
            index_handle: IndexHandle::default(),
        }
    }
}

impl TablePropertiesCollector for SizePropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        let size = match get_entry_size(value, entry_type) {
            Ok(entry_size) => key.len() as u64 + entry_size,
            Err(_) => return,
        };
        self.index_handle.size += size;
        self.index_handle.offset += size as u64;
        // Add the start key for convenience.
        if self.last_key.is_empty() || self.index_handle.size >= DEFAULT_PROP_SIZE_INDEX_DISTANCE {
            self.props
                .index_handles
                .insert(key.to_owned(), self.index_handle.clone());
            self.index_handle.size = 0;
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        self.props.total_size = self.index_handle.offset;
        if self.index_handle.size > 0 {
            self.props
                .index_handles
                .insert(self.last_key.clone(), self.index_handle.clone());
        }
        self.props.encode().0
    }
}

#[derive(Default)]
pub struct SizePropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory for SizePropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<dyn TablePropertiesCollector> {
        Box::new(SizePropertiesCollector::new())
    }
}

pub struct UserProperties(HashMap<Vec<u8>, Vec<u8>>);

impl Deref for UserProperties {
    type Target = HashMap<Vec<u8>, Vec<u8>>;
    fn deref(&self) -> &HashMap<Vec<u8>, Vec<u8>> {
        &self.0
    }
}

impl DerefMut for UserProperties {
    fn deref_mut(&mut self) -> &mut HashMap<Vec<u8>, Vec<u8>> {
        &mut self.0
    }
}

impl UserProperties {
    fn new() -> UserProperties {
        UserProperties(HashMap::new())
    }

    fn encode(&mut self, name: &str, value: Vec<u8>) {
        self.insert(name.as_bytes().to_owned(), value);
    }

    pub fn encode_u64(&mut self, name: &str, value: u64) {
        let mut buf = Vec::with_capacity(8);
        buf.encode_u64(value).unwrap();
        self.encode(name, buf);
    }

    pub fn encode_handles(&mut self, name: &str, handles: &IndexHandles) {
        self.encode(name, handles.encode())
    }
}

pub trait DecodeProperties {
    fn decode(&self, k: &str) -> Result<&[u8]>;

    fn decode_u64(&self, k: &str) -> Result<u64> {
        let mut buf = self.decode(k)?;
        number::decode_u64(&mut buf)
    }

    fn decode_handles(&self, k: &str) -> Result<IndexHandles> {
        let buf = self.decode(k)?;
        IndexHandles::decode(buf)
    }
}

impl DecodeProperties for UserProperties {
    fn decode(&self, k: &str) -> Result<&[u8]> {
        match self.0.get(k.as_bytes()) {
            Some(v) => Ok(v.as_slice()),
            None => Err(Error::KeyNotFound),
        }
    }
}

impl DecodeProperties for UserCollectedProperties {
    fn decode(&self, k: &str) -> Result<&[u8]> {
        match self.get(k.as_bytes()) {
            Some(v) => Ok(v),
            None => Err(Error::KeyNotFound),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RangeOffsets {
    size: u64,
    keys: u64,
}

#[derive(Debug, Default)]
pub struct RangeProperties {
    pub offsets: BTreeMap<Vec<u8>, RangeOffsets>,
}

impl RangeProperties {
    pub fn encode(&self) -> UserProperties {
        let mut buf = Vec::with_capacity(1024);
        for (k, offsets) in &self.offsets {
            buf.encode_u64(k.len() as u64).unwrap();
            buf.extend(k);
            buf.encode_u64(offsets.size).unwrap();
            buf.encode_u64(offsets.keys).unwrap();
        }
        let mut props = UserProperties::new();
        props.encode(PROP_RANGE_INDEX, buf);
        props
    }
}

impl From<SizeProperties> for RangeProperties {
    fn from(p: SizeProperties) -> RangeProperties {
        let mut res = RangeProperties::default();
        for (key, size_handle) in p.index_handles.0 {
            let mut range = RangeOffsets::default();
            range.size = size_handle.offset;
            res.offsets.insert(key, range);
        }
        res
    }
}

pub struct RangePropertiesCollector {
    props: RangeProperties,
    last_offsets: RangeOffsets,
    last_key: Vec<u8>,
    cur_offsets: RangeOffsets,
    prop_size_index_distance: u64,
    prop_keys_index_distance: u64,
}

impl Default for RangePropertiesCollector {
    fn default() -> Self {
        RangePropertiesCollector {
            props: RangeProperties::default(),
            last_offsets: RangeOffsets::default(),
            last_key: vec![],
            cur_offsets: RangeOffsets::default(),
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl RangePropertiesCollector {
    pub fn new(prop_size_index_distance: u64, prop_keys_index_distance: u64) -> Self {
        RangePropertiesCollector {
            prop_size_index_distance,
            prop_keys_index_distance,
            ..Default::default()
        }
    }

    fn size_in_last_range(&self) -> u64 {
        self.cur_offsets.size - self.last_offsets.size
    }

    fn keys_in_last_range(&self) -> u64 {
        self.cur_offsets.keys - self.last_offsets.keys
    }

    fn insert_new_point(&mut self, key: Vec<u8>) {
        self.last_offsets = self.cur_offsets.clone();
        self.props.offsets.insert(key, self.cur_offsets.clone());
    }
}

impl TablePropertiesCollector for RangePropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        // size
        let size = match get_entry_size(value, entry_type) {
            Ok(entry_size) => key.len() as u64 + entry_size,
            Err(_) => return,
        };
        self.cur_offsets.size += size;
        // keys
        self.cur_offsets.keys += 1;
        // Add the start key for convenience.
        if self.last_key.is_empty()
            || self.size_in_last_range() >= self.prop_size_index_distance
            || self.keys_in_last_range() >= self.prop_keys_index_distance
        {
            self.insert_new_point(key.to_owned());
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        if self.size_in_last_range() > 0 || self.keys_in_last_range() > 0 {
            let key = self.last_key.clone();
            self.insert_new_point(key);
        }
        self.props.encode().0
    }
}

pub struct RangePropertiesCollectorFactory {
    pub prop_size_index_distance: u64,
    pub prop_keys_index_distance: u64,
}

impl Default for RangePropertiesCollectorFactory {
    fn default() -> Self {
        RangePropertiesCollectorFactory {
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        }
    }
}

impl TablePropertiesCollectorFactory for RangePropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<dyn TablePropertiesCollector> {
        Box::new(RangePropertiesCollector::new(
            self.prop_size_index_distance,
            self.prop_keys_index_distance,
        ))
    }
}
