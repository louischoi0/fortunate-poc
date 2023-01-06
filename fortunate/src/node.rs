use std::collections::HashMap;
use rand::Rng;
use std::{thread, time};
use log::{debug, error, info, trace, warn};

use crate::dynamoc;
use crate::tsgen;
use crate::tsgen::TsEpochPair;
use crate::flog::FortunateLogger;
use crate::cursor::{ Cursor };

use aws_sdk_dynamodb::{ Client, };
use aws_sdk_dynamodb::{
  model::AttributeValue, Error
};


#[derive(Debug)]
enum FNodeStatus {
  RUNNING,
  DISCONNECTED,
  WAIT,
}

#[derive(Debug)]
pub struct FNodeUUIDGenerator {}

#[derive(Debug)]
pub struct FlagKernal {
  f: fn (u32) -> u32
}

impl FlagKernal {
  pub fn new(f: fn (u32) -> u32) -> Self {
    return FlagKernal { f: f };
  }
}

#[derive(Debug)]
pub struct FlagSet {
  f0: u16,
  f1: u16,
  f2: u16,
  f3: u16,
}

impl FlagSet {
  pub fn new() -> Self {
    return FlagSet {
      f0: 0, f1: 0, f2: 0, f3: 0
    }
  }

  pub fn update(&mut self, f0: u16, f1: u16, f2: u16, f3: u16) {
    self.f0 = f0;
    self.f1 = f1;
    self.f2 = f2;
    self.f3 = f3;
  }

  pub fn to_string(&self) -> String {
    self.f0.to_string() + &self.f1.to_string() + &self.f2.to_string() + &self.f3.to_string()
  }
}

#[derive(std::clone::Clone, Debug)]
pub struct NodeSignalKey {
  pub signal_key: std::string::String,
  pub epoch: std::string::String,
  pub ts: tsgen::Timestamp,
  pub flags: std::string::String,
}

impl NodeSignalKey {
  pub fn and(&self, other: &NodeSignalKey, flagidx: usize) -> bool {
    let a = String::from(self.flags.chars().nth(flagidx).unwrap()).parse::<u8>().unwrap();
    let b = String::from(other.flags.chars().nth(flagidx).unwrap()).parse::<u8>().unwrap();

    return !((a*b) == 0)
  }

  pub fn at(&self, flagidx: usize) -> bool {
    !(String::from(self.flags.chars().nth(flagidx).unwrap()).parse::<u8>().unwrap() == 0)
  }

}

#[derive(std::clone::Clone, Debug)]
pub struct NodeSignal {
  pub epoch: std::string::String,
  pub signal_key: std::string::String,
  pub group: std::string::String,
  pub timestamp: String, 
  pub signal_value: String, 

  pub data: std::option::Option<String>,
}



impl NodeSignal {

  pub fn parse_key(d: &String) -> NodeSignalKey {
    let mut cursor = Cursor::new(d.to_owned());

    let epoch = cursor.advance(6);
    let ts = tsgen::Timestamp::from_str(&cursor.advance(16));

    let flags = cursor.advance(4);

    NodeSignalKey { 
      signal_key: d[0..d.len()-4].to_string(),
      epoch:epoch, 
      ts: ts, 
      flags: flags
    }
  }
}


#[derive(Debug)]
pub struct FNode {
  uuid: String,
  host: String,
  
  flags_s: u16,
  pub flagset: FlagSet,
  flagset_updated_ts: u16,

  seed: u16,

  interval: u16,
  kernels: Vec<FlagKernal>,

  status: FNodeStatus,
  last_status_updated: u16,

  group_hash: String,
  dynamo_client: Client,

  logger: FortunateLogger,
}

impl FNode{
  /**
  pub async fn get_node_signals(client: &Client, epoch: &std::string::String) -> Vec<NodeSignalKey> {

  } */

  pub async fn get_node_s_signals(client: &Client, epoch: &String) -> Vec<HashMap<String, AttributeValue>> {
    let mut q = HashMap::<String,String>::new();
    let _nodesignal_dimpl = dynamoc::DynamoHandler::nodesignal();

    q.insert(String::from("table_name"), String::from("node_signals"));
    q.insert(String::from("key_column"), String::from("epoch"));
    q.insert(String::from("query_value"), epoch.to_owned());

    let items = _nodesignal_dimpl.query(client, q).await.unwrap();

    let items_u = items.unwrap();

    for v in items_u.iter() {
      let data = v.get(&String::from("data"));
    }

    items_u
  }

  pub async fn new(uuid: String) -> Self {
    return FNode {
      uuid: uuid,
      host: String::from("localhost"),
      flags_s: 0,
      flagset: FlagSet::new(),
      flagset_updated_ts: 0,
      seed: 0,
      interval: 0,
      kernels: Vec::new(),
      status: FNodeStatus::RUNNING,
      last_status_updated: 0,
      group_hash: String::from(""),
      dynamo_client: dynamoc::get_dynamo_client().await,

      logger: FortunateLogger::new(String::from("node")),
    }
  } 

  fn init_kernels(&mut self) {
    self.kernels.push(FlagKernal::new(|x:u32|  x & 1 ));
    self.kernels.push(FlagKernal::new(|x:u32|  x & 2 ));
    self.kernels.push(FlagKernal::new(|x:u32|  x & 4 ));
    self.kernels.push(FlagKernal::new(|x:u32|  x & 8 ));
    self.kernels.push(FlagKernal::new(|x:u32|  x & 16 ));
  }

  pub fn update(&mut self) {
    let mut rng = rand::thread_rng();
    //self.flagset_updated_ts = tsgen::unix_epoch();

    self.seed = rng.gen::<u16>();
    self.update_flag();
  }

  pub fn make_signal(&self) -> NodeSignal {
    let mut data = HashMap::<String, String>::new();

    let tspair = tsgen::get_ts_pair();
    let TsEpochPair {ts, epoch} = tspair;
    let nodedata = String::from(&epoch) + &ts + &self.flagset.to_string();

    NodeSignal { 
      epoch: epoch.to_owned(), 
      signal_key: epoch + &ts, 
      group: String::from(""), 
      timestamp: ts, 
      signal_value: self.flagset.to_string(), 
      data: Option::Some(nodedata),
    }
  }

  pub async fn insert_node_signal(&self) -> Result<(), Error>{
    let hdr = dynamoc::DynamoHandler::node();
    let mut data = HashMap::<String, String>::new();

    let signal = self.make_signal();

    data.insert(String::from("epoch"), signal.epoch.to_owned());
    data.insert(String::from("signal_key"), signal.signal_key);
    data.insert(String::from("group"), signal.group);
    data.insert(String::from("timestamp"), signal.timestamp);
    data.insert(String::from("signal_value"), signal.signal_value);
    data.insert(String::from("data"), signal.data.to_owned().unwrap());

    let request = hdr.make_insert_request(&self.dynamo_client, data);
    println!("node signal inserted: {} {}", signal.epoch, signal.data.unwrap().to_owned());
    hdr.commit(request).await?;
    Ok(())
  }

  fn update_flag(&mut self) {
    let f0 = if (self.seed as f32 % 2.0 == 0.0) { 1 } else { 0 };
    let f1 = if (self.seed as f32 % 4.0 == 0.0) { 1 } else { 0 };
    let f2 = if (self.seed as f32 % 8.0 == 0.0) { 1 } else { 0 };
    let f3 = if (self.seed as f32 % 16.0 == 0.0) { 1 } else { 0 };

    self.flagset.update(
      f0, 
      f1, 
      f2, 
      f3
    )
  }

  pub async fn process(&mut self) {
    let interval = time::Duration::from_millis(15000);

    loop {
      let mut msg = String::from("update flag: ");
      msg.push_str(self.flagset.to_string().as_str());

      self.logger.info(msg.as_str());
      self.update();
      self.insert_node_signal().await;

      thread::sleep(interval);
    }

  }


}




