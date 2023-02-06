use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use rand::Rng;
use log::{debug, error, info, trace, warn};
use redis::Commands;

use crate::primitives::DataType;
use crate::sessions::RedisImpl;
use crate::{dynamoc, matrix};
use crate::matrix::ObjectSession;
use crate::tsgen;
use crate::tsgen::TsEpochPair;
use crate::flog::FortunateLogger;
use crate::cursor::{ Cursor, TCursor };
use async_trait::async_trait;

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
}

enum SignalType {
  BITARR(NodeSignalBitArr),
  NUMERIC(),
}

#[derive(std::clone::Clone, Debug)]
pub struct NodeSignalBase {
  pub epoch: std::string::String,
  pub signal_key: std::string::String,
  pub group: std::string::String,
  pub timestamp: String, 
  pub signal_value: String, 

  pub data: std::option::Option<String>,
}

pub struct NodeSignalBitArr {
  pub base: NodeSignalBase,
  pub flags: String,
}

pub struct NodeSignalNumeric<T> {
  pub base: NodeSignalBase,
  pub seed: T,
}

trait BitOp<T> {
  fn and(&self, other: T) -> bool;
}

trait BitReduceOp<T> {
  fn and(v: Vec<&T>) -> bool;
}

impl BitOp<NodeSignalBitArr> for NodeSignalBitArr  {
  fn and(&self, other: NodeSignalBitArr) -> bool {
    true
  }
}



impl NodeSignalBase {

  pub fn parse_key(d: &String) -> NodeSignalKey {
    let mut cursor = Cursor::new(d);

    let epoch = cursor.advance(6);
    let ts = tsgen::Timestamp::from_str(&cursor.advance(16));

    NodeSignalKey { 
      signal_key: d[0..d.len()-4].to_string(),
      epoch:epoch, 
      ts: ts, 
    }
  }
}

pub struct FNode {
  pub uuid: String,
  pub host: String,

  pub region: String, 
  node_type: String,
  
  flags_s: u16,
  pub flagset: FlagSet,
  pub session: crate::matrix::ObjectSession,

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

#[async_trait]
pub trait TNode<S> {
  async fn update(&mut self);
  fn make_signal(&self) -> NodeSignalBase;

}

impl FNode{
  /**
  pub async fn get_node_signals(client: &Client, epoch: &std::string::String) -> Vec<NodeSignalKey> {
  } */

  pub async fn get_node_s_signals(client: &Client, epoch: &String) -> Vec<HashMap<String, DataType>> {
    let _nodesignal_dimpl = dynamoc::DynamoHandler::nodesignal();

    let qctx = crate::dynamoc::DynamoSelectQueryContext {
      table_name: &"node_signals",
      conditions: Some(vec![
        crate::primitives::Pair::<&'static str, crate::primitives::DataType> {
            k: "epoch",
            v: crate::primitives::DataType::S(epoch.to_owned()),
        },
      ]),
      query_subtype: dynamoc::DynamoSelectQuerySubType::All
    };

    let items = 
        _nodesignal_dimpl.q(
        &client, 
        &qctx
      ).await.unwrap();

    match items {
      dynamoc::SelectQuerySetResult::All(x) => x.unwrap(),
      _ => panic!("invalid select operation type.")
    }
  }

  pub async fn new(uuid: &std::string::String, region: &std::string::String) -> Self {
    //TODO make function mapping to region based on statics
    return FNode {
      uuid: uuid.to_owned(),
      host: String::from("localhost"),

      region: region.to_owned(),
      node_type: std::string::String::from("SN"),

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

      session: ObjectSession::new(uuid.to_owned(), String::from("N")),
      logger: FortunateLogger::new("node"),
    }
  } 

  pub fn update(&mut self) {
    let mut rng = rand::thread_rng();

    self.seed = rng.gen::<u16>();
    self.update_flag();
  }

  pub fn make_signal(&self) -> NodeSignalBase {
    let tspair = tsgen::get_ts_pair();
    let TsEpochPair {ts, epoch} = tspair;
    let nodedata = String::from(&epoch) + &ts + &self.flagset.to_string();

    NodeSignalBase { 
      epoch: epoch.to_owned(), 
      signal_key: epoch + &ts, 
      group: String::from(""), 
      timestamp: ts, 
      signal_value: self.flagset.to_string(), 
      data: Option::Some(nodedata),
    }
  }

  pub fn get_node_session(imp: &mut RedisImpl, nodeid: &std::string::String) -> HashMap<String, String> {
    let mut data = HashMap::<String, String>::new();

    data.insert(
      std::string::String::from("last_signal_emitted"),
      imp.redis_connection.get::<String, String>(
        std::string::String::from(
          format!("{}:last_signal_emitted", nodeid)
        )
      ).unwrap() 
    );

    data
  }

  pub async fn insert_node_signal(&mut self) -> Result<(), Error> {

    self.logger.info(
      format!("Acquire for {} from {}; mod:{:?}", &self.region, &self.uuid, 0)
            .as_str()
    );

    crate::matrix::ObjectLock::acquire(
      &mut self.session.cimpl,
      &self.region,
      &self.uuid,
      0
    ).await;

    let hdr = dynamoc::DynamoHandler::node();
    let mut data = HashMap::<String, String>::new();

    let signal = self.make_signal();

    data.insert(String::from("epoch"), signal.epoch.to_owned());
    data.insert(String::from("signal_key"), signal.signal_key);
    data.insert(String::from("region"), signal.group);
    data.insert(String::from("timestamp"), signal.timestamp);
    data.insert(String::from("signal_value"), signal.signal_value);
    data.insert(String::from("data"), signal.data.to_owned().unwrap());

    let last_singal_emitted = tsgen::get_time();
    let request = hdr.make_insert_request(&self.dynamo_client, data);
    
    self.logger.info(
      format!(
        "{} node signal inserted: {} {}", 
        last_singal_emitted, 
        signal.epoch, 
        signal.data.unwrap().to_owned()
      ).as_str());

    self.session.cimpl.set::<String, String>(
      std::string::String::from("last_signal_emitted"),
      last_singal_emitted
    );

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

  pub async fn spawn_process() {

  }


  pub fn terminate_node(&mut self) {
    self.session.set_status(
      &std::string::String::from("TERMINATED")
    );

    matrix::Matrix::terminate_node_callback(self);
  }

  pub async fn update_session(&mut self) { }

  pub fn execute_flag(&mut self) -> bool {
    let flag = self.session.get_flag();

    if ( (&flag).eq("TERMINATED") ) {
      self.terminate_node();
      true
    }

    else {
      false 
    }

  }

  pub async fn process(&mut self) {
    let loop_interval = 1000;
    let update_inteval = 15;
    let mut frame_cnt = 0;

    self.session.initialize();
    matrix::Matrix::new_node_callback(self);

    loop {
      self.update_session();
      if ( self.execute_flag() ) {
        self.logger.info("node has been shutdown.");
        break;
      }

      if (frame_cnt % 15 == 0) {
        let mut msg = String::from("update flag: ");
        
        msg.push_str(self.flagset.to_string().as_str());
        self.logger.info(msg.as_str());

        self.update();
        self.insert_node_signal().await;
        self.session.timestamp();
      }
      
      frame_cnt = (frame_cnt + 1) % update_inteval;

      sleep(Duration::from_millis(loop_interval)).await;
    }

  }


}



