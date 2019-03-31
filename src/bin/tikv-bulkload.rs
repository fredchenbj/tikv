use tikv::import::client::Client;
use tikv::import::common;
use tikv::import::prepare;
use std::mem;

fn main() {
    let pd_addr = "10.136.16.1:2379";
    let client = Client::new(pd_addr, 1);

    println!("{}", mem::size_of_val(&client));

    let mut start = Vec::new();
    let mut k = Vec::new();

    let range = common::RangeInfo::new(&start, &k, 0);

    let tag = format!("[PrepareRangeJob {}:{}]",  0, 0);
    let job = prepare::PrepareRangeJob::new(tag, range, client);
    job.run();
    
}