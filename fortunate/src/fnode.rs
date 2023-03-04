use std::collections::HashMap;
use tokio::time::{sleep, Duration};

use aws_sdk_dynamodb::{ Client, };
use async_trait::async_trait;
use sha2::digest::typenum::Bit;

use crate::cursor::{Cursor, TCursor};
use crate::flog::FortunateLogger;
use crate::matrix::ObjectSession;
use crate::sessions::RedisImpl;
use crate::{dynamoc, tsgen, hashlib};
use crate::primitives::DataType;
use crate::window::{TBitWindow, WindowInitializer, BitWindow, WindowInitializable, WindowShufflable};
use crate::cursor::{DimensionWindowCursor, DCursor};

const NodeLoggerS01: FortunateLogger = FortunateLogger::new("node_s01");

pub async fn get_node_s_signals(
  client: &Client, 
  epoch: &String
) -> Vec<HashMap<String, DataType>> {

  NodeLoggerS01.info(
    format!("op:get_node_s_signals; epoch:{}", epoch).as_str()
  );

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


pub trait ISignalKey {

}

pub struct BitArraySignalKey {
  data: String,
}

impl BitArraySignalKey {

  pub fn new(data: &std::string::String) -> Self {
    BitArraySignalKey{
      data: data.to_owned()
    }
  }

}

#[async_trait]
pub trait INode { 
  fn _interval(&mut self); 
  fn emit(&mut self) -> HashMap<String, String>;
  fn signalbuffer(&mut self) -> String;
  async fn commit(&mut self, data: &HashMap<String, String>);

  async fn process(&mut self) {
    let loop_interval = 1000;
    let emit_inteval = 15;
    let mut frame_cnt: u64 = 0;

    loop {

      if (frame_cnt % emit_inteval == 0) {
        self._interval();
        let data = self.emit();
        self.commit(&data).await;
      }

      frame_cnt += 1;
      sleep(Duration::from_millis(loop_interval)).await;
    }
  }
}

pub trait INodeSession { }

pub struct INodeImpl_S01<W: TBitWindow, V: Clone + Copy> {
  dynamo_client: Client,
  s: V, //phantom value
  cursor2: DimensionWindowCursor<W>,
  cursor10: DimensionWindowCursor<W>,

  uuid: std::string::String,
  region: std::string::String,

  session: ObjectSession,
  logger: &'static FortunateLogger,
}

impl INodeImpl_S01<BitWindow, u16> {

  pub async fn new(region: &std::string::String) -> Self {
    let mut v = vec![];
    let mut v2 = vec![];

    let wi = WindowInitializer::<BitWindow>::new();
    let uuid = hashlib::uuid(6);

    // InodeImpl_S01 have 10 windows for 2^x
    for i in (1..11) {
      v.push(
        wi.create(1024, i32::pow(2,i).try_into().unwrap())
      );
    };

    // InodeImpl_S01 have 3 windows for 10^x
    for i in (1..4) {
      v2.push(
        wi.create(1000, i32::pow(10,i).try_into().unwrap())
      );
    };

    INodeImpl_S01 { 
      dynamo_client: dynamoc::get_dynamo_client().await, 
      s: 0,
      cursor2: DimensionWindowCursor::new(v),
      cursor10: DimensionWindowCursor::new(v2),

      uuid: uuid.to_owned(),
      region: std::string::String::from(""),

      session: ObjectSession::new(
        uuid.to_owned(),
        std::string::String::from("nodes01"),
      ),

      logger: &NodeLoggerS01
    }
  }
}

fn get_node_signal_hm(
  epoch: &String,
  signalbuffer: &String,
  ts: &String,
  uuid: &String 
) -> HashMap<String, String> {
  let mut data = HashMap::<String, String>::new();

  data.insert(
    String::from("epoch"), epoch.to_owned()
  );

  data.insert(
    String::from("signal_key"), signalbuffer.to_owned()
  );

  data.insert(
    String::from("timestamp"), ts.to_owned()
  );

  data.insert(
    String::from("epoch"), epoch.to_owned()
  );

  data.insert(
    String::from("node_uuid"), uuid.to_owned()
  );

  data
}

#[async_trait]
impl INode for INodeImpl_S01<BitWindow, u16> {

  fn _interval(&mut self) {
    for _w in self.cursor2.wdw_ref.iter_mut() {
      WindowInitializer::<BitWindow>::shuffle_bw__fisher_yates(_w);
    }

    for _w in self.cursor10.wdw_ref.iter_mut() {
      WindowInitializer::<BitWindow>::shuffle_bw__fisher_yates(_w);
    }
  }

  async fn commit(&mut self, data: &HashMap<String, String>) {
    let hdr = dynamoc::DynamoHandler::node();
    let request = hdr.make_insert_request(&self.dynamo_client, data.to_owned());

    hdr.commit(request).await;

    self.logger.info(
      format!("op=send_signal; data={:?};", data).as_str()
    );

    self.session.timestamp();
  }

  fn emit(&mut self) -> HashMap<String, String> {
    let signalbuffer = self.signalbuffer();
    let mut c = Cursor::new(&signalbuffer);

    let epoch = c.epoch();
    let uuid = c.advance(6);
    let ts = c.timestamp();

    let region = std::string::String::from(""); //TODO

    get_node_signal_hm(&epoch, &signalbuffer, &ts, &uuid)
  }

  fn signalbuffer(&mut self) -> String {
    let crate::tsgen::TsEpochPair {ts, epoch} = crate::tsgen::get_ts_pair();

    let s = self.cursor2.advance();
    let s2 = self.cursor10.advance();
    epoch + &self.uuid + &ts + &s + &s2
  }

  
}

