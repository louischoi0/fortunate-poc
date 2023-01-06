use crate::node::{ FNode };
use crate::tsgen::{ TsEpochPair, get_ts_pair, self };
use env;
use redis::Commands;

use log::{debug, info};

extern crate redis;

struct RedisClient {
}

pub struct FortunateGroupSession {
  group_uuid: String,

  epoch: String,
  prev_epoch: String,

  is_finalizing: bool,  
  
  redis_client: redis::Client,
  redis_connection: redis::Connection,
}

impl FortunateGroupSession {
  pub fn new() -> Self {
    let TsEpochPair { ts, epoch } = get_ts_pair();
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut con = client.get_connection().unwrap();

    FortunateGroupSession {
      group_uuid: String::from("abcdefg"),
      epoch: epoch,
      prev_epoch: String::from(""),
      is_finalizing: false,
      redis_client: client,
      redis_connection: con,
    }
  }

  pub fn init_group_session(&mut self) {
    self.commit_epoch();
    let prev_epoch = tsgen::get_prev_epoch();
    assert!(self.epoch != prev_epoch);
    self.prev_epoch = prev_epoch;
    self.commit_prev_epoch();
  }

  pub fn commit_epoch(&mut self) {
    self.redis_set(String::from("epoch"), self.epoch.to_owned());
  }

  pub fn commit_prev_epoch(&mut self) {
    self.redis_set(String::from("prev_epoch"), self.prev_epoch.to_owned());
  }

  pub fn redis_set(&mut self, key: String, value: String) {
    let rkey = format!("{}#{}", self.group_uuid, key);
    self.redis_connection.set::<String, String, String>(rkey, value).unwrap();
  }

}
