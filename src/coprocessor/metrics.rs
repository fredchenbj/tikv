// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::*;

pub struct TlsCop {
    pub COPR_REQ_HISTOGRAM_VEC: HistogramVec,
    pub OUTDATED_REQ_WAIT_TIME: HistogramVec,
    pub COPR_REQ_HANDLE_TIME: HistogramVec,
    pub COPR_REQ_WAIT_TIME: HistogramVec,
    pub COPR_REQ_ERROR: IntCounterVec,
    pub COPR_SCAN_KEYS: HistogramVec,
    pub COPR_SCAN_DETAILS: IntCounterVec,
    pub COPR_ROCKSDB_PERF_COUNTER: IntCounterVec,
    pub COPR_EXECUTOR_COUNT: IntCounterVec,
    pub COPR_GET_OR_SCAN_COUNT: IntCounterVec,
}

lazy_static! {
    pub static ref tls_metrics: TlsCop = {
        let st = TlsCop {
            COPR_REQ_HISTOGRAM_VEC: register_histogram_vec!(
                "tikv_coprocessor_request_duration_seconds",
                "Bucketed histogram of coprocessor request duration",
                &["req"],
                exponential_buckets(0.0005, 2.0, 20).unwrap()
            )
            .unwrap(),
            OUTDATED_REQ_WAIT_TIME: register_histogram_vec!(
                "tikv_coprocessor_outdated_request_wait_seconds",
                "Bucketed histogram of outdated coprocessor request wait duration",
                &["req"],
                exponential_buckets(0.0005, 2.0, 20).unwrap()
            )
            .unwrap(),
            COPR_REQ_HANDLE_TIME: register_histogram_vec!(
                "tikv_coprocessor_request_handle_seconds",
                "Bucketed histogram of coprocessor handle request duration",
                &["req"],
                exponential_buckets(0.0005, 2.0, 20).unwrap()
            )
            .unwrap(),
            COPR_REQ_WAIT_TIME: register_histogram_vec!(
                "tikv_coprocessor_request_wait_seconds",
                "Bucketed histogram of coprocessor request wait duration",
                &["req"],
                exponential_buckets(0.0005, 2.0, 20).unwrap()
            )
            .unwrap(),
            COPR_REQ_ERROR: register_int_counter_vec!(
                "tikv_coprocessor_request_error",
                "Total number of push down request error.",
                &["reason"]
            )
            .unwrap(),
            COPR_SCAN_KEYS: register_histogram_vec!(
                "tikv_coprocessor_scan_keys",
                "Bucketed histogram of coprocessor per request scan keys",
                &["req"],
                exponential_buckets(1.0, 2.0, 20).unwrap()
            )
            .unwrap(),
            COPR_SCAN_DETAILS: register_int_counter_vec!(
                "tikv_coprocessor_scan_details",
                "Bucketed counter of coprocessor scan details for each CF",
                &["req", "cf", "tag"]
            )
            .unwrap(),
            COPR_ROCKSDB_PERF_COUNTER: register_int_counter_vec!(
                "tikv_coprocessor_rocksdb_perf",
                "Total number of RocksDB internal operations from PerfContext",
                &["req", "metric"]
            )
            .unwrap(),
            COPR_EXECUTOR_COUNT: register_int_counter_vec!(
                "tikv_coprocessor_executor_count",
                "Total number of each executor",
                &["type"]
            )
            .unwrap(),
            COPR_GET_OR_SCAN_COUNT: register_int_counter_vec!(
                "tikv_coprocessor_get_or_scan_count",
                "Total number of rocksdb query of get or scan count",
                &["type"]
            )
            .unwrap(),
        };
        st
    };
}
