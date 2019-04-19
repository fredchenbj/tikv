use std::sync::Arc;
use tikv::import::client::*;
use tikv::import::common::RangeInfo;
use tikv::import::prepare::PrepareRangeJob;
use tikv::import::Result;

use clap::{crate_authors, crate_version, App, Arg};

//fn new_encoded_key(k: &[u8]) -> Vec<u8> {
//    if k.is_empty() {
//        vec![]
//    } else {
//        k.iter().cloned().collect()
//    }
//}

pub fn split_start_key(key: &[u8], prefix: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + key.len());
    v.push(prefix);
    // v.extend_from_slice(&[prefix]);
    v.extend_from_slice(key);
    v.push(':' as u8);
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
    //  let range = RangeInfo::new(&start_key, &end_key, 0);

    let tag = format!("[PrepareRangeJob {}:{}]", 0, 0);
    let job = PrepareRangeJob::new(tag, range, Arc::clone(&client));
    job.run()
}

fn main() {
    // ./tikv-bulkload --pd 10.136.16.1:2379 --table-name "table" --shard-bits 2
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
            Arg::with_name("table-name")
                .long("table-name")
                .takes_value(true)
                .help("Set the table name"),
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

    let client = Client::new(pd_addr, 1);
    let client = match client {
        Ok(item) => item,
        Err(e) => panic!(e),
    };
    let client = Arc::new(client);
    //println!("{}", mem::size_of_val(&client));

    let mut table_name = "table";
    if let Some(tablename) = matches.value_of("table-name") {
        table_name = tablename;
    }

    //let mut shard_key_bits: u8 = 2;
    let shard_key_bits = matches.value_of("shard-bits").map_or(1, |s| s.parse().expect("parse u8"));

    let max_count: u8 = 1 << shard_key_bits;

    for i in 0..max_count {
        let new_key_prefix = i << (8 - shard_key_bits - 1);
        let start_key = split_start_key(table_name.as_bytes(), new_key_prefix);
        let end_key = split_end_key(table_name.as_bytes(), new_key_prefix);

        //println!("start_key: {:?}", start_key);
        //println!("end_key: {:?}", end_key);

        let start = Vec::new();
        let first_range = RangeInfo::new(&start, &start_key, 0);
        let res = split_and_scatter_region(first_range, Arc::clone(&client));
        match res {
            Ok(x) => println!("first job {} runs {}", i, x),
            Err(_) => println!("first job {} error", i),
        };

        let second_range = RangeInfo::new(&start_key, &end_key, 0);
        let res = split_and_scatter_region(second_range, Arc::clone(&client));
        match res {
            Ok(x) => println!("second job {} runs {}", i, x),
            Err(_) => println!("second job {} error", i),
        };
    }
}
