// Copyright 2019 PingCAP, Inc.
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

use std::cell::RefCell;
use std::time::Duration;

use prometheus::local::*;

use crate::pd::PdTask;
use crate::server::readpool::{self, Builder, ReadPool};
use crate::util::collections::HashMap;
use crate::util::worker::FutureScheduler;

use super::metrics::*;

pub struct TlsStorage {
    local_sched_histogram_vec: RefCell<LocalHistogramVec>,
    local_sched_processing_read_histogram_vec: RefCell<LocalHistogramVec>,
    local_kv_command_keyread_histogram_vec: RefCell<LocalHistogramVec>,
    local_kv_command_counter_vec: RefCell<LocalIntCounterVec>,
    local_sched_commands_pri_counter_vec: RefCell<LocalIntCounterVec>,
    local_kv_command_scan_details: RefCell<LocalIntCounterVec>,
    local_read_flow_stats: RefCell<HashMap<u64, crate::storage::FlowStatistics>>,
}

thread_local! {
    static TLS_STORAGE_METRICS: TlsStorage = TlsStorage {
        local_sched_histogram_vec: RefCell::new(SCHED_HISTOGRAM_VEC.local()),
        local_sched_processing_read_histogram_vec: RefCell::new(SCHED_PROCESSING_READ_HISTOGRAM_VEC.local()),
        local_kv_command_keyread_histogram_vec: RefCell::new(KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local()),
        local_kv_command_counter_vec: RefCell::new(KV_COMMAND_COUNTER_VEC.local()),
        local_sched_commands_pri_counter_vec: RefCell::new(SCHED_COMMANDS_PRI_COUNTER_VEC.local()),
        local_kv_command_scan_details: RefCell::new(KV_COMMAND_SCAN_DETAILS.local()),
        local_read_flow_stats: RefCell::new(HashMap::default()),
    }
}

pub struct ReadPoolImpl;

impl ReadPoolImpl {
    #[inline]
    pub fn build_read_pool(
        config: &readpool::Config,
        pd_sender: FutureScheduler<PdTask>,
        name_prefix: &str,
    ) -> ReadPool {
        let pd_sender2 = pd_sender.clone();

        Builder::from_config(config)
            .name_prefix(name_prefix)
            .on_tick(move || ReadPoolImpl::tls_flush(&pd_sender))
            .before_stop(move || ReadPoolImpl::tls_flush(&pd_sender2))
            .build()
    }

    #[inline]
    fn tls_flush(pd_sender: &FutureScheduler<PdTask>) {
        TLS_STORAGE_METRICS.with(|m| {
            // Flush Prometheus metrics
            m.local_sched_histogram_vec.borrow_mut().flush();
            m.local_sched_processing_read_histogram_vec
                .borrow_mut()
                .flush();
            m.local_kv_command_keyread_histogram_vec
                .borrow_mut()
                .flush();
            m.local_kv_command_counter_vec.borrow_mut().flush();
            m.local_sched_commands_pri_counter_vec.borrow_mut().flush();
            m.local_kv_command_scan_details.borrow_mut().flush();

            // Report PD metrics
            if m.local_read_flow_stats.borrow().is_empty() {
                // Stats to report to PD is empty, ignore.
                return;
            }

            let read_stats = m.local_read_flow_stats.replace(HashMap::default());
            let result = pd_sender.schedule(PdTask::ReadStats { read_stats });
            if let Err(e) = result {
                error!("Failed to send read pool read flow statistics"; "err" => ?e);
            }
        });
    }

    #[inline]
    pub fn tls_collect_command_count(cmd: &str, priority: readpool::Priority) {
        TLS_STORAGE_METRICS.with(|m| {
            m.local_kv_command_counter_vec
                .borrow_mut()
                .with_label_values(&[cmd])
                .inc();
            m.local_sched_commands_pri_counter_vec
                .borrow_mut()
                .with_label_values(&[priority.as_str()])
                .inc();
        });
    }

    #[inline]
    pub fn tls_collect_command_duration(cmd: &str, duration: Duration) {
        TLS_STORAGE_METRICS.with(|m| {
            m.local_sched_histogram_vec
                .borrow_mut()
                .with_label_values(&[cmd])
                .observe(crate::util::time::duration_to_sec(duration))
        });
    }

    #[inline]
    pub fn tls_collect_key_reads(cmd: &str, count: usize) {
        TLS_STORAGE_METRICS.with(|m| {
            m.local_kv_command_keyread_histogram_vec
                .borrow_mut()
                .with_label_values(&[cmd])
                .observe(count as f64)
        });
    }

    #[inline]
    pub fn tls_processing_read_observe_duration<F, R>(cmd: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        TLS_STORAGE_METRICS.with(|m| {
            let now = crate::util::time::Instant::now_coarse();
            let ret = f();
            m.local_sched_processing_read_histogram_vec
                .borrow_mut()
                .with_label_values(&[cmd])
                .observe(now.elapsed_secs());
            ret
        })
    }

    #[inline]
    pub fn tls_collect_scan_count(cmd: &str, statistics: &crate::storage::Statistics) {
        TLS_STORAGE_METRICS.with(|m| {
            let mut histogram = m.local_kv_command_scan_details.borrow_mut();
            for (cf, details) in statistics.details() {
                for (tag, count) in details {
                    histogram
                        .with_label_values(&[cmd, cf, tag])
                        .inc_by(count as i64);
                }
            }
        });
    }

    #[inline]
    pub fn tls_collect_read_flow(region_id: u64, statistics: &crate::storage::Statistics) {
        TLS_STORAGE_METRICS.with(|m| {
            let mut map = m.local_read_flow_stats.borrow_mut();
            let flow_stats = map
                .entry(region_id)
                .or_insert_with(crate::storage::FlowStatistics::default);
            flow_stats.add(&statistics.write.flow_stats);
            flow_stats.add(&statistics.data.flow_stats);
        });
    }
}
