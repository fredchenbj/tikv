// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::mem;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

use json;
use reqwest;
use serde_json::Value;

use crate::rocks::{
    BlockBasedOptions, Cache, ColumnFamilyOptions, CompactionPriority, DBCompactionStyle,
    DBCompressionType, DBTitanDBBlobRunMode, LRUCacheOptions, MergeOperands, TitanDBOptions,
};
use tikv_util::config::ReadableSize;

const DEFAULT_PROP_SIZE_INDEX_DISTANCE: u64 = 4 * 1024 * 1024;
const DEFAULT_PROP_KEYS_INDEX_DISTANCE: u64 = 40 * 1024;

const TABLE_LEN: usize = 4;

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompressionType {
    No,
    Snappy,
    Zlib,
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
    ZstdNotFinal,
}

impl From<CompressionType> for DBCompressionType {
    fn from(compression_type: CompressionType) -> DBCompressionType {
        match compression_type {
            CompressionType::No => DBCompressionType::No,
            CompressionType::Snappy => DBCompressionType::Snappy,
            CompressionType::Zlib => DBCompressionType::Zlib,
            CompressionType::Bz2 => DBCompressionType::Bz2,
            CompressionType::Lz4 => DBCompressionType::Lz4,
            CompressionType::Lz4hc => DBCompressionType::Lz4hc,
            CompressionType::Zstd => DBCompressionType::Zstd,
            CompressionType::ZstdNotFinal => DBCompressionType::ZstdNotFinal,
        }
    }
}

pub mod compression_type_level_serde {
    use std::fmt;

    use serde::de::{Error, SeqAccess, Unexpected, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserializer, Serializer};

    use crate::rocks::DBCompressionType;

    pub fn serialize<S>(ts: &[DBCompressionType; 7], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(Some(ts.len()))?;
        for t in ts {
            let name = match *t {
                DBCompressionType::No => "no",
                DBCompressionType::Snappy => "snappy",
                DBCompressionType::Zlib => "zlib",
                DBCompressionType::Bz2 => "bzip2",
                DBCompressionType::Lz4 => "lz4",
                DBCompressionType::Lz4hc => "lz4hc",
                DBCompressionType::Zstd => "zstd",
                DBCompressionType::ZstdNotFinal => "zstd-not-final",
                DBCompressionType::Disable => "disable",
            };
            s.serialize_element(name)?;
        }
        s.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[DBCompressionType; 7], D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SeqVisitor;
        impl<'de> Visitor<'de> for SeqVisitor {
            type Value = [DBCompressionType; 7];

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a compression type vector")
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<[DBCompressionType; 7], S::Error>
            where
                S: SeqAccess<'de>,
            {
                let mut seqs = [DBCompressionType::No; 7];
                let mut i = 0;
                while let Some(value) = seq.next_element::<String>()? {
                    if i == 7 {
                        return Err(S::Error::invalid_value(
                            Unexpected::Str(&value),
                            &"only 7 compression types",
                        ));
                    }
                    seqs[i] = match &*value.trim().to_lowercase() {
                        "no" => DBCompressionType::No,
                        "snappy" => DBCompressionType::Snappy,
                        "zlib" => DBCompressionType::Zlib,
                        "bzip2" => DBCompressionType::Bz2,
                        "lz4" => DBCompressionType::Lz4,
                        "lz4hc" => DBCompressionType::Lz4hc,
                        "zstd" => DBCompressionType::Zstd,
                        "zstd-not-final" => DBCompressionType::ZstdNotFinal,
                        "disable" => DBCompressionType::Disable,
                        _ => {
                            return Err(S::Error::invalid_value(
                                Unexpected::Str(&value),
                                &"invalid compression type",
                            ));
                        }
                    };
                    i += 1;
                }
                if i < 7 {
                    return Err(S::Error::invalid_length(i, &"7 compression types"));
                }
                Ok(seqs)
            }
        }

        deserializer.deserialize_seq(SeqVisitor)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlobRunMode {
    Normal,
    ReadOnly,
    Fallback,
}

impl Into<DBTitanDBBlobRunMode> for BlobRunMode {
    fn into(self) -> DBTitanDBBlobRunMode {
        match self {
            BlobRunMode::Normal => DBTitanDBBlobRunMode::Normal,
            BlobRunMode::ReadOnly => DBTitanDBBlobRunMode::ReadOnly,
            BlobRunMode::Fallback => DBTitanDBBlobRunMode::Fallback,
        }
    }
}

macro_rules! numeric_enum_mod {
    ($name:ident $enum:ident { $($variant:ident = $value:expr, )* }) => {
        pub mod $name {
            use std::fmt;

            use serde::{Serializer, Deserializer};
            use serde::de::{self, Unexpected, Visitor};
            use crate::rocks::$enum;

            pub fn serialize<S>(mode: &$enum, serializer: S) -> Result<S::Ok, S::Error>
                where S: Serializer
            {
                serializer.serialize_i64(*mode as i64)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<$enum, D::Error>
                where D: Deserializer<'de>
            {
                struct EnumVisitor;

                impl<'de> Visitor<'de> for EnumVisitor {
                    type Value = $enum;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        write!(formatter, concat!("valid ", stringify!($enum)))
                    }

                    fn visit_i64<E>(self, value: i64) -> Result<$enum, E>
                        where E: de::Error
                    {
                        match value {
                            $( $value => Ok($enum::$variant), )*
                            _ => Err(E::invalid_value(Unexpected::Signed(value), &self))
                        }
                    }
                }

                deserializer.deserialize_i64(EnumVisitor)
            }

            #[cfg(test)]
            mod tests {
                use toml;
                use crate::rocks::$enum;

                #[test]
                fn test_serde() {
                    #[derive(Serialize, Deserialize, PartialEq)]
                    struct EnumHolder {
                        #[serde(with = "super")]
                        e: $enum,
                    }

                    let cases = vec![
                        $(($enum::$variant, $value), )*
                    ];
                    for (e, v) in cases {
                        let holder = EnumHolder { e };
                        let res = toml::to_string(&holder).unwrap();
                        let exp = format!("e = {}\n", v);
                        assert_eq!(res, exp);
                        let h: EnumHolder = toml::from_str(&exp).unwrap();
                        assert!(h == holder);
                    }
                }
            }
        }
    }
}

numeric_enum_mod! {compaction_pri_serde CompactionPriority {
    ByCompensatedSize = 0,
    OldestLargestSeqFirst = 1,
    OldestSmallestSeqFirst = 2,
    MinOverlappingRatio = 3,
}}

numeric_enum_mod! {rate_limiter_mode_serde DBRateLimiterMode {
    ReadOnly = 1,
    WriteOnly = 2,
    AllIo = 3,
}}

numeric_enum_mod! {compaction_style_serde DBCompactionStyle {
    Level = 0,
    Universal = 1,
}}

numeric_enum_mod! {recovery_mode_serde DBRecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
    PointInTime = 2,
    SkipAnyCorruptedRecords = 3,
}}

macro_rules! build_cf_opt {
    ($opt:ident, $cache:ident) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        if let Some(cache) = $cache {
            block_base_opts.set_block_cache(cache);
        } else {
            let mut cache_opts = LRUCacheOptions::new();
            cache_opts.set_capacity($opt.block_cache_size.0 as usize);
            block_base_opts.set_block_cache(&Cache::new_lru_cache(cache_opts));
        }
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts
            .set_pin_l0_filter_and_index_blocks_in_cache($opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            block_base_opts.set_bloom_filter(
                $opt.bloom_filter_bits_per_key,
                $opt.block_based_bloom_filter,
            );
            block_base_opts.set_whole_key_filtering($opt.whole_key_filtering);
        }
        block_base_opts.set_read_amp_bytes_per_bit($opt.read_amp_bytes_per_bit);
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.set_num_levels($opt.num_levels);
        assert!($opt.compression_per_level.len() >= $opt.num_levels as usize);
        let compression_per_level = $opt.compression_per_level[..$opt.num_levels as usize].to_vec();
        cf_opts.compression_per_level(compression_per_level.as_slice());
        cf_opts.set_write_buffer_size($opt.write_buffer_size.0);
        cf_opts.set_max_write_buffer_number($opt.max_write_buffer_number);
        cf_opts.set_min_write_buffer_number_to_merge($opt.min_write_buffer_number_to_merge);
        cf_opts.set_max_bytes_for_level_base($opt.max_bytes_for_level_base.0);
        cf_opts.set_target_file_size_base($opt.target_file_size_base.0);
        cf_opts.set_level_zero_file_num_compaction_trigger($opt.level0_file_num_compaction_trigger);
        cf_opts.set_level_zero_slowdown_writes_trigger($opt.level0_slowdown_writes_trigger);
        cf_opts.set_level_zero_stop_writes_trigger($opt.level0_stop_writes_trigger);
        cf_opts.set_max_compaction_bytes($opt.max_compaction_bytes.0);
        cf_opts.compaction_priority($opt.compaction_pri);
        cf_opts.set_level_compaction_dynamic_level_bytes($opt.dynamic_level_bytes);
        cf_opts.set_max_bytes_for_level_multiplier($opt.max_bytes_for_level_multiplier);
        cf_opts.set_compaction_style($opt.compaction_style);
        cf_opts.set_disable_auto_compactions($opt.disable_auto_compactions);
        cf_opts.set_soft_pending_compaction_bytes_limit($opt.soft_pending_compaction_bytes_limit.0);
        cf_opts.set_hard_pending_compaction_bytes_limit($opt.hard_pending_compaction_bytes_limit.0);
        cf_opts.set_optimize_filters_for_hits($opt.optimize_filters_for_hits);
        cf_opts.add_merge_operator("update operator", update_merge);

        cf_opts
    }};
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TitanCfConfig {
    pub min_blob_size: ReadableSize,
    pub blob_file_compression: CompressionType,
    pub blob_cache_size: ReadableSize,
    pub min_gc_batch_size: ReadableSize,
    pub max_gc_batch_size: ReadableSize,
    pub discardable_ratio: f64,
    pub sample_ratio: f64,
    pub merge_small_file_threshold: ReadableSize,
}

impl Default for TitanCfConfig {
    fn default() -> Self {
        Self {
            min_blob_size: ReadableSize::kb(1), // disable titan default
            blob_file_compression: CompressionType::Lz4,
            blob_cache_size: ReadableSize::mb(0),
            min_gc_batch_size: ReadableSize::mb(16),
            max_gc_batch_size: ReadableSize::mb(64),
            discardable_ratio: 0.5,
            sample_ratio: 0.1,
            merge_small_file_threshold: ReadableSize::mb(8),
        }
    }
}

impl TitanCfConfig {
    fn build_opts(&self) -> TitanDBOptions {
        let mut opts = TitanDBOptions::new();
        opts.set_min_blob_size(self.min_blob_size.0 as u64);
        opts.set_blob_file_compression(self.blob_file_compression.into());
        opts.set_blob_cache(self.blob_cache_size.0 as usize, -1, false, 0.0);
        opts.set_min_gc_batch_size(self.min_gc_batch_size.0 as u64);
        opts.set_max_gc_batch_size(self.max_gc_batch_size.0 as u64);
        opts.set_discardable_ratio(self.discardable_ratio);
        opts.set_sample_ratio(self.sample_ratio);
        opts.set_merge_small_file_threshold(self.merge_small_file_threshold.0 as u64);
        opts
    }
}

macro_rules! cf_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            pub disable_block_cache: bool,
            pub cache_index_and_filter_blocks: bool,
            pub pin_l0_filter_and_index_blocks: bool,
            pub use_bloom_filter: bool,
            pub optimize_filters_for_hits: bool,
            pub whole_key_filtering: bool,
            pub bloom_filter_bits_per_key: i32,
            pub block_based_bloom_filter: bool,
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "compression_type_level_serde")]
            pub compression_per_level: [DBCompressionType; 7],
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub level0_file_num_compaction_trigger: i32,
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: i32,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "compaction_pri_serde")]
            pub compaction_pri: CompactionPriority,
            pub dynamic_level_bytes: bool,
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
            #[serde(with = "compaction_style_serde")]
            pub compaction_style: DBCompactionStyle,
            pub disable_auto_compactions: bool,
            pub soft_pending_compaction_bytes_limit: ReadableSize,
            pub hard_pending_compaction_bytes_limit: ReadableSize,
            pub prop_size_index_distance: u64,
            pub prop_keys_index_distance: u64,
            pub titan: TitanCfConfig,
        }
    };
}

cf_config!(RawCfConfig);

impl Default for RawCfConfig {
    fn default() -> RawCfConfig {
        RawCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(512),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(64),
            max_write_buffer_number: 100,
            min_write_buffer_number_to_merge: 2,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(32),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 100,
            level0_stop_writes_trigger: 300,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
            compaction_style: DBCompactionStyle::Level,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(64),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(256),
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_keys_index_distance: DEFAULT_PROP_KEYS_INDEX_DISTANCE,
            titan: TitanCfConfig::default(),
        }
    }
}

impl RawCfConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self, cache);
        let f = Box::new(super::properties::RangePropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_keys_index_distance: self.prop_keys_index_distance,
        });
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        cf_opts.set_titandb_options(&self.titan.build_opts());
        cf_opts
    }
}

pub fn get_raw_cf_option(cf: &str, cache: &Option<Cache>) -> (ColumnFamilyOptions, i32) {
    assert_eq!(cf.len(), (TABLE_LEN + 1) * 2);
    let mut ttl = 0;
    let cf = &cf[0..TABLE_LEN * 2];
    debug!("new cf: {}", cf);
    let mut config = self::RawCfConfig::default();
    let pds = get_pd_endpoints();
    let mut res = String::from("");

    for pd in pds {
        let url = format!("{}{}{}{}", "http://", pd, "/v2/keys/", cf);
        debug!("url: {}", url);
        let resp = reqwest::get(url.as_str());
        if resp.is_ok() {
            res = resp.unwrap().text().unwrap();
            break;
        }
    }

    let u = json::parse(&res);
    let json_value;
    let res_option = if u.is_ok() {
        json_value = u.unwrap();
        let json_array = &json_value["node"]["nodes"];

        let len = json_array.len();
        let mut max_key = json_array[0]["key"].as_str().unwrap();
        let mut max_index = 0;
        for i in 1..len {
            let key = json_array[i]["key"].as_str().unwrap();
            if key > max_key {
                max_key = key;
                max_index = i;
            }
        }
        debug!("max key: {}, max index: {}", max_key, max_index);
        json_value["node"]["nodes"][max_index]["value"].as_str()
    } else {
        None
    };

    if res_option.is_some() {
        let str = res_option.unwrap();
        let v: Value = serde_json::from_str(str).unwrap();

        let m = v.as_object().unwrap();
        for (key, value) in m {
            info!("{}: {}", key, value.as_str().unwrap());
            match key.as_str() {
                "block_size" => {
                    if let Some(v) = value.as_str() {
                        let size = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid block_size: {}", e);
                            64
                        });
                        config.block_size = ReadableSize::kb(size);
                    }
                }
                "block_cache_size" => {
                    if let Some(v) = value.as_str() {
                        let size = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid block_cache_size: {}", e);
                            512
                        });
                        config.block_cache_size = ReadableSize::mb(size);
                    }
                }
                "disable_block_cache" => {
                    if let Some(v) = value.as_str() {
                        config.disable_block_cache = v.parse::<bool>().unwrap_or_else(|e| {
                            warn!("invalid disable_block_cache: {}", e);
                            false
                        });
                    }
                }
                "cache_index_and_filter_blocks" => {
                    if let Some(v) = value.as_str() {
                        config.cache_index_and_filter_blocks =
                            v.parse::<bool>().unwrap_or_else(|e| {
                                warn!("invalid cache_index_and_filter_blocks: {}", e);
                                true
                            });
                    }
                }
                "pin_l0_filter_and_index_blocks" => {
                    if let Some(v) = value.as_str() {
                        config.pin_l0_filter_and_index_blocks =
                            v.parse::<bool>().unwrap_or_else(|e| {
                                warn!("invalid pin_l0_filter_and_index_blocks: {}", e);
                                true
                            });
                    }
                }
                "use_bloom_filter" => {
                    if let Some(v) = value.as_str() {
                        config.use_bloom_filter = v.parse::<bool>().unwrap_or_else(|e| {
                            warn!("invalid use_bloom_filter: {}", e);
                            true
                        });
                    }
                }
                "optimize_filters_for_hits" => {
                    if let Some(v) = value.as_str() {
                        config.optimize_filters_for_hits = v.parse::<bool>().unwrap_or_else(|e| {
                            warn!("invalid optimize_filters_for_hits: {}", e);
                            false
                        });
                    }
                }
                "whole_key_filtering" => {
                    if let Some(v) = value.as_str() {
                        config.whole_key_filtering = v.parse::<bool>().unwrap_or_else(|e| {
                            warn!("invalid whole_key_filtering: {}", e);
                            true
                        });
                    }
                }
                "bloom_filter_bits_per_key" => {
                    if let Some(v) = value.as_str() {
                        config.bloom_filter_bits_per_key = v.parse::<i32>().unwrap_or_else(|e| {
                            warn!("invalid bloom_filter_bits_per_key: {}", e);
                            10
                        });
                    }
                }
                "block_based_bloom_filter" => {
                    if let Some(v) = value.as_str() {
                        config.block_based_bloom_filter = v.parse::<bool>().unwrap_or_else(|e| {
                            warn!("invalid block_based_bloom_filter: {}", e);
                            false
                        });
                    }
                }
                "read_amp_bytes_per_bit" => {
                    if let Some(v) = value.as_str() {
                        config.read_amp_bytes_per_bit = v.parse::<u32>().unwrap_or_else(|e| {
                            warn!("invalid read_amp_bytes_per_bit: {}", e);
                            0
                        });
                    }
                }
                "write_buffer_size" => {
                    if let Some(v) = value.as_str() {
                        let size = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid write_buffer_size: {}", e);
                            64
                        });
                        config.write_buffer_size = ReadableSize::mb(size);
                    }
                }
                "max_write_buffer_number" => {
                    if let Some(v) = value.as_str() {
                        config.max_write_buffer_number = v.parse::<i32>().unwrap_or_else(|e| {
                            warn!("invalid max_write_buffer_number: {}", e);
                            100
                        });
                    }
                }
                "min_write_buffer_number_to_merge" => {
                    if let Some(v) = value.as_str() {
                        config.min_write_buffer_number_to_merge =
                            v.parse::<i32>().unwrap_or_else(|e| {
                                warn!("invalid min_write_buffer_number_to_merge: {}", e);
                                2
                            });
                    }
                }
                "max_bytes_for_level_base" => {
                    if let Some(v) = value.as_str() {
                        let size = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid max_bytes_for_level_base: {}", e);
                            512
                        });
                        config.max_bytes_for_level_base = ReadableSize::mb(size);
                    }
                }
                "target_file_size_base" => {
                    if let Some(v) = value.as_str() {
                        let size = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid target_file_size_base: {}", e);
                            32
                        });
                        config.target_file_size_base = ReadableSize::mb(size);
                    }
                }
                "level0_file_num_compaction_trigger" => {
                    if let Some(v) = value.as_str() {
                        config.level0_file_num_compaction_trigger =
                            v.parse::<i32>().unwrap_or_else(|e| {
                                warn!("invalid level0_file_num_compaction_trigger: {}", e);
                                4
                            });
                    }
                }
                "level0_slowdown_writes_trigger" => {
                    if let Some(v) = value.as_str() {
                        config.level0_slowdown_writes_trigger =
                            v.parse::<i32>().unwrap_or_else(|e| {
                                warn!("invalid level0_slowdown_writes_trigger: {}", e);
                                100
                            });
                    }
                }
                "level0_stop_writes_trigger" => {
                    if let Some(v) = value.as_str() {
                        config.level0_stop_writes_trigger = v.parse::<i32>().unwrap_or_else(|e| {
                            warn!("invalid level0_stop_writes_trigger: {}", e);
                            300
                        });
                    }
                }
                "max_compaction_bytes" => {
                    if let Some(v) = value.as_str() {
                        let size = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid max_compaction_bytes: {}", e);
                            2
                        });
                        config.max_compaction_bytes = ReadableSize::gb(size);
                    }
                }
                "dynamic_level_bytes" => {
                    if let Some(v) = value.as_str() {
                        config.dynamic_level_bytes = v.parse::<bool>().unwrap_or_else(|e| {
                            warn!("invalid dynamic_level_bytes: {}", e);
                            true
                        });
                    }
                }
                "num_levels" => {
                    if let Some(v) = value.as_str() {
                        config.num_levels = v.parse::<i32>().unwrap_or_else(|e| {
                            warn!("invalid num_levels: {}", e);
                            7
                        });
                    }
                }
                "max_bytes_for_level_multiplier" => {
                    if let Some(v) = value.as_str() {
                        config.max_bytes_for_level_multiplier =
                            v.parse::<i32>().unwrap_or_else(|e| {
                                warn!("invalid max_bytes_for_level_multiplier: {}", e);
                                10
                            });
                    }
                }
                "disable_auto_compactions" => {
                    if let Some(v) = value.as_str() {
                        config.disable_auto_compactions = v.parse::<bool>().unwrap_or_else(|e| {
                            warn!("invalid disable_auto_compactions: {}", e);
                            false
                        });
                    }
                }
                "soft_pending_compaction_bytes_limit" => {
                    if let Some(v) = value.as_str() {
                        let size = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid soft_pending_compaction_bytes_limit: {}", e);
                            64
                        });
                        config.soft_pending_compaction_bytes_limit = ReadableSize::gb(size);
                    }
                }
                "hard_pending_compaction_bytes_limit" => {
                    if let Some(v) = value.as_str() {
                        let size = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid hard_pending_compaction_bytes_limit: {}", e);
                            256
                        });
                        config.hard_pending_compaction_bytes_limit = ReadableSize::gb(size);
                    }
                }
                "prop_size_index_distance" => {
                    if let Some(v) = value.as_str() {
                        config.prop_size_index_distance = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid prop_size_index_distance: {}", e);
                            DEFAULT_PROP_SIZE_INDEX_DISTANCE
                        });
                    }
                }
                "prop_keys_index_distance" => {
                    if let Some(v) = value.as_str() {
                        config.prop_keys_index_distance = v.parse::<u64>().unwrap_or_else(|e| {
                            warn!("invalid prop_keys_index_distance: {}", e);
                            DEFAULT_PROP_KEYS_INDEX_DISTANCE
                        });
                    }
                }
                "ttl" => {
                    let mut t = 0;
                    if let Some(v) = value.as_str() {
                        t = v.parse::<i32>().unwrap_or_else(|e| {
                            warn!("invalid ttl: {}", e);
                            0
                        });
                    }
                    info!("ttl: {}", t);
                    if t > 0 {
                        ttl = t;
                    }
                }
                _ => {
                    info!("default path");
                }
            }
        }
    }

    (config.build_opt(cache), ttl)
}

pub fn get_pd_endpoints() -> Vec<String> {
    let path: &str = "pd.txt";
    let input: File = File::open(path).unwrap();
    let buffered: BufReader<File> = BufReader::new(input);

    let mut endpoints: Vec<String> = Vec::new();
    for line in buffered.lines().map(|x| x.unwrap()) {
        endpoints.push(line);
    }
    endpoints
}

/**
|total_size|key1_size|key1|value1_size|value1|key2_size|key2|value2_size|value2|...
all size is four bytes.
existing_value : existing_val
operand_list   : operands
new_value      : result
Note: little endian Coding
*/
fn update_merge(_: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::with_capacity(operands.size_hint().0);
    let arr: [u8; 4] = [0, 0, 0, 0];
    result.extend_from_slice(&arr[..]);

    let mut map_index = HashMap::new();

    let mut total_size: u32 = 0;
    total_size += 4;

    let mut v = Vec::new();
    for op in operands {
        v.push(op);
    }
    let len = v.len();

    for i in 0..len {
        let op_value: &[u8] = v.get(len - 1 - i).unwrap();
        let mut begin = 0;
        begin += 4;

        let op_len = op_value.len();
        while begin < op_len {
            //decode key size
            let first = begin;
            let end = begin + 4;
            let vv = &op_value[begin..end];
            let ptr: *const u8 = vv.as_ptr();
            let ptr: *const u32 = ptr as *const u32;
            let key_size = unsafe { *ptr };

            //decode key
            begin = end;
            let end = begin + key_size as usize;
            let key = &op_value[begin..end];

            //decode value size
            begin = end;
            let end = begin + 4;
            let vv = &op_value[begin..end];
            let ptr: *const u8 = vv.as_ptr();
            let ptr: *const u32 = ptr as *const u32;
            let value_size = unsafe { *ptr };

            //decode value
            begin = end;
            let end = begin + value_size as usize;
            //let value = &op_value[begin..end];

            if let None = map_index.get(&key) {
                map_index.entry(key).or_insert(1);

                if value_size != 0 {
                    result.extend_from_slice(&op_value[first..end]);
                    total_size += 8 + key_size + value_size;
                }
            }
            begin = end;
        }
    }

    if let Some(existing_value) = existing_val {
        let mut begin = 0;
        let exist_total_size = existing_value.len();

        begin += 4;

        while begin < exist_total_size {
            //decode key size
            let first = begin;
            let end = begin + 4;
            let vv = &existing_value[begin..end];
            let ptr: *const u8 = vv.as_ptr();
            let ptr: *const u32 = ptr as *const u32;
            let key_size = unsafe { *ptr };

            //decode key
            begin = end;
            let end = begin + key_size as usize;
            let key = &existing_value[begin..end];

            //decode value size
            begin = end;
            let end = begin + 4;
            let vv = &existing_value[begin..end];
            let ptr: *const u8 = vv.as_ptr();
            let ptr: *const u32 = ptr as *const u32;
            let value_size = unsafe { *ptr };

            //decode value
            begin = end;
            let end = begin + value_size as usize;
            //let value = &existing_value[begin..end];

            if let Some(_) = map_index.get(&key) {
                begin = end;
                continue;
            }

            //if value_size != 0 {
            result.extend_from_slice(&existing_value[first..end]);
            total_size += 8 + key_size + value_size;
            //}
            begin = end;
        }
    }

    let res_u8;
    unsafe {
        res_u8 = mem::transmute::<u32, [u8; 4]>(total_size);
    }
    for i in 0..4 {
        if let Some(elem) = result.get_mut(i) {
            *elem = res_u8[i];
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rocks::DBCompressionType;

    #[test]
    fn test_parse_compression_type() {
        #[derive(Serialize, Deserialize)]
        struct CompressionTypeHolder {
            #[serde(with = "compression_type_level_serde")]
            tp: [DBCompressionType; 7],
        }

        let all_tp = vec![
            (DBCompressionType::No, "no"),
            (DBCompressionType::Snappy, "snappy"),
            (DBCompressionType::Zlib, "zlib"),
            (DBCompressionType::Bz2, "bzip2"),
            (DBCompressionType::Lz4, "lz4"),
            (DBCompressionType::Lz4hc, "lz4hc"),
            (DBCompressionType::Zstd, "zstd"),
            (DBCompressionType::ZstdNotFinal, "zstd-not-final"),
            (DBCompressionType::Disable, "disable"),
        ];
        for i in 0..all_tp.len() - 7 {
            let mut src = [DBCompressionType::No; 7];
            let mut exp = ["no"; 7];
            for (i, &t) in all_tp[i..i + 7].iter().enumerate() {
                src[i] = t.0;
                exp[i] = t.1;
            }
            let holder = CompressionTypeHolder { tp: src };
            let res_str = toml::to_string(&holder).unwrap();
            let exp_str = format!("tp = [\"{}\"]\n", exp.join("\", \""));
            assert_eq!(res_str, exp_str);
            let h: CompressionTypeHolder = toml::from_str(&exp_str).unwrap();
            assert_eq!(h.tp, holder.tp);
        }

        // length is wrong.
        assert!(toml::from_str::<CompressionTypeHolder>("tp = [\"no\"]").is_err());
        assert!(toml::from_str::<CompressionTypeHolder>(
            r#"tp = [
            "no", "no", "no", "no", "no", "no", "no", "no"
        ]"#
        )
        .is_err());
        // value is wrong.
        assert!(toml::from_str::<CompressionTypeHolder>(
            r#"tp = [
            "no", "no", "no", "no", "no", "no", "yes"
        ]"#
        )
        .is_err());
    }
}
