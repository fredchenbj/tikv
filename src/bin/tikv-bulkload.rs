use tikv::import::client::*;
use tikv::import::common::RangeInfo;
use tikv::import::prepare::PrepareRangeJob;
//use std::mem;
use std::sync::Arc;

fn new_encoded_key(k: &[u8]) -> Vec<u8> {
    if k.is_empty() {
        vec![]
    } else {
        k.iter().cloned().collect()
    }
}

fn main() {
    let pd_addr = "10.136.16.1:2379";
    let client = Client::new(pd_addr, 1);

    let client = match client {
        Ok(item) => item,
        Err(e) => panic!(e),
    };
    //println!("{}", mem::size_of_val(&client));

    let start = Vec::new();
    let k = new_encoded_key(b"test:5");

    let range = RangeInfo::new(&start, &k, 0);

    let tag = format!("[PrepareRangeJob {}:{}]",  0, 0);
    let job = PrepareRangeJob::new(tag, range, Arc::new(client));
    let res = job.run();
    match res {
        Ok(x) => println!("job runs {}", x),
        Err(_) => println!("job error"),  // here 
    };
}