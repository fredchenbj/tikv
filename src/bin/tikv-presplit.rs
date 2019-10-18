#[macro_use(
    kv,
    slog_kv,
    slog_warn,
    slog_info,
    slog_debug,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;
extern crate slog_async;
#[macro_use]
extern crate slog_global;
extern crate slog_term;

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use tikv::import::client::*;
use tikv::import::common::*;
use tikv::import::prepare::PrepareRangeJob;
use tikv::import::Result;

use clap::{crate_authors, crate_version, App, Arg};

const SCATTER_WAIT_MAX_RETRY_TIMES: u64 = 128;
const SCATTER_WAIT_INTERVAL_MILLIS: u64 = 50;
const SCATTER_MAX_WAIT_INTERVAL_MILLIS: u64 = 5000;

macro_rules! exec_with_retry {
    ($tag:expr, $func:expr, $times:expr, $interval:expr, $max_duration:expr) => {
        let start = Instant::now();
        let mut interval = $interval;
        for i in 0..$times {
            if $func {
                if i > 0 {
                    debug!(concat!("waited between ", $tag); "retry times" => %i, "takes" => ?start.elapsed());
                }
                break;
            } else if i == $times - 1 {
                warn!(concat!($tag, " still failed after exhausting all retries"));
            } else {
                // Exponential back-off with max wait duration
                interval = (2 * interval).min($max_duration);
                thread::sleep(Duration::from_millis(interval));
            }
        }
    };
}

pub fn split_start_key(table_key: &[u8], shard_byte: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + table_key.len());
    v.extend_from_slice(table_key);
    v.push(shard_byte);
    v
}

pub fn split_end_key(key: &[u8], prefix: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + key.len());
    v.push(prefix);
    v.extend_from_slice(key);
    v.push(';' as u8);
    v
}

pub fn split_and_scatter_region(range: RangeInfo, client: Arc<Client>) -> Result<bool> {
    let mut wait_scatter_regions = vec![];

    let tag = format!("[PrepareRangeJob {}:{}]", 0, 0);
    let job = PrepareRangeJob::new(tag, range, Arc::clone(&client));
    job.run(&mut wait_scatter_regions)?;

    // We need to wait all regions for scattering finished.
    let start = Instant::now();
    while let Some(region_id) = wait_scatter_regions.pop() {
        exec_with_retry!(
            "scatter",
            client.is_scatter_region_finished(region_id)?,
            SCATTER_WAIT_MAX_RETRY_TIMES,
            SCATTER_WAIT_INTERVAL_MILLIS,
            SCATTER_MAX_WAIT_INTERVAL_MILLIS
        );
    }
    info!("scatter this region finished"; "takes" => ?start.elapsed());
    Ok(true)
}

fn main() {
    let matches = App::new("TiKV BulkLoad")
        .about("bulk load for TiKV")
        .author(crate_authors!())
        .version(crate_version!())
        .arg(
            Arg::with_name("pd")
                .long("pd")
                .takes_value(true)
                .help("Set the address of pd"),
        )
        .arg(
            Arg::with_name("table-id")
                .long("table-id")
                .takes_value(true)
                .help("Set the table id"),
        )
        .arg(
            Arg::with_name("shard-bits")
                .long("shard-bits")
                .takes_value(true)
                .help("Set the shard key bits"),
        )
        .get_matches();

    let mut pd_addr = "10.136.16.2:2379";
    if let Some(pd) = matches.value_of("pd") {
        pd_addr = pd;
    }

    let min_available_ratio = 0.05;
    let client = Client::new(pd_addr, 1, min_available_ratio);
    let client = match client {
        Ok(item) => item,
        Err(e) => panic!(e),
    };
    let client = Arc::new(client);

    let mut table_id = Vec::new();
    if let Some(t) = matches.value_of("table-id") {
        table_id = hex::decode(t).unwrap();

        if table_id.len() != tikv::raftstore::store::keys::TABLE_LEN {
            println!("the length of table is wrong");
            return;
        }
    }

    let shard_key_bits = matches
        .value_of("shard-bits")
        .map_or(0, |s| s.parse().expect("parse u8"));

    let max_count: u8 = 1 << shard_key_bits;

    for i in 0..max_count {
        let shard_byte = i;
        let start_key = split_start_key(&table_id, shard_byte);

        let range = RangeInfo::new(&start_key, &start_key, 0);
        let res = split_and_scatter_region(range, Arc::clone(&client));
        match res {
            Ok(x) => println!("job {} runs {}", i, x),
            Err(_) => println!("job {} error", i),
        };
    }
}
