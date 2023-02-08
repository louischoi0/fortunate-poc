use core::panic;

use crate::node::{ FNode };
use crate::primitives::DataType;
use crate::tsgen::{ TsEpochPair, get_ts_pair, self };
use env;

use redis::{Commands, FromRedisValue, RedisResult, ToRedisArgs};
use crate::flog::FortunateLogger;

use log::{debug, info};

extern crate redis;

pub struct RedisImpl {
  redis_client: redis::Client,
  pub redis_connection: redis::Connection,
  prefix_key: Option<String>,

  logger: FortunateLogger
}

const REDIS_HOST: &'static str = "redis://127.0.0.1/";

impl RedisImpl {
  pub fn new(prefix_key: Option<String>) -> Self {
    let client = redis::Client::open(REDIS_HOST).unwrap();
    let mut con = client.get_connection().unwrap();

    RedisImpl { 
      redis_client: client,
      redis_connection: con,
      prefix_key: prefix_key,

      logger: FortunateLogger::new("redisimpl"),
    }
  }

  pub fn bind_prefix_key(&self, key: String) -> String {

    match &self.prefix_key {
      Some(x) => {
         x.to_owned() + ":" + &key
      }
      None => key
    }

  }

  pub fn get<T: FromRedisValue> (&mut self, key: String) -> T{
    let _key = self.bind_prefix_key(key);
    self.redis_connection.get::<String, T>(_key).unwrap()
  }

  pub fn set<'a, V: ToRedisArgs, T: FromRedisValue>(
    &mut self, 
    key: String, 
    value: V
  ) -> () {
    let _key = self.bind_prefix_key(key.to_owned());
    self.logger.info(
      format!("set key {:?} to ", _key.to_owned().as_str()).as_str()
    );

    self.redis_connection.set::<'a, String, V, T>(_key, value).unwrap();
  }
}


struct SessionCacheImpl {

}
