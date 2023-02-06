use std::collections::HashMap;

use aws_sdk_dynamodb::{ Client, };
use async_trait::async_trait;
use sha2::digest::typenum::Bit;

use crate::cursor::Cursor;
use crate::sessions::RedisImpl;
use crate::{dynamoc, tsgen};
use crate::primitives::DataType;
use crate::window::{TBitWindow, WindowInitializer, BitWindow, WindowInitializable, WindowShufflable};
use crate::cursor::{DimensionWindowCursor, DCursor};

pub async fn get_node_s_signals(
  client: &Client, 
  epoch: &String
) -> Vec<HashMap<String, DataType>> {

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
  async fn emit(&mut self, cimpl: &mut RedisImpl);
  fn signalbuffer(&mut self) -> String;
}

pub struct INodeImpl_X01<W: TBitWindow, V: Clone + Copy> {
  dynamo_client: Client,
  w: W,
  s: V,
}

impl INodeImpl_X01<BitWindow, u16> {
  pub async fn new() -> Self {
    let wi = WindowInitializer::<BitWindow>::new();
    INodeImpl_X01 {
      dynamo_client: dynamoc::get_dynamo_client().await,
      w: wi.create(100, 2),
      s: 0,
    }
  }

}

#[async_trait]
impl INode for INodeImpl_X01<BitWindow, u16> { 
  fn _interval(&mut self) {
    WindowInitializer::<BitWindow>::shuffle_bw__fisher_yates(&mut self.w);
  }

  async fn emit(&mut self, cimpl: &mut RedisImpl) {
    let crate::tsgen::TsEpochPair {ts, epoch} = crate::tsgen::get_ts_pair();

  }

  fn signalbuffer(&mut self) -> String {
    String::from("")    
  }

}

pub struct INodeImpl_S01<W: TBitWindow, V: Clone + Copy> {
  dynamo_client: Client,
  s: V, //phantom value
  cursor: DimensionWindowCursor<W>,

  uuid: std::string::String,
  region: std::string::String,
}

impl INodeImpl_S01<BitWindow, u16> {

  async fn new() -> Self {
    let mut v = vec![];
    let wi = WindowInitializer::<BitWindow>::new();

    // InodeImpl_S01 have 10 windows
    for i in (1..11) {
      v.push(
        wi.create(1024, 2^i)
      );
    };

    INodeImpl_S01 { 
      dynamo_client: dynamoc::get_dynamo_client().await, 
      s: 0,
      cursor: DimensionWindowCursor::new(v),

      uuid: std::string::String::from(""),
      region: std::string::String::from(""),
    }
  }

}

#[async_trait]
impl INode for INodeImpl_S01<BitWindow, u16> {
  fn _interval(&mut self) {
    for _w in self.cursor.wdw_ref.iter_mut() {
      WindowInitializer::<BitWindow>::shuffle_bw__fisher_yates(_w);
    }
  }

  async fn emit(&mut self, cimpl: &mut RedisImpl) {
    let signalbuffer = self.signalbuffer();

    let mut c = Cursor::new(&signalbuffer);
    let epoch = c.epoch();

    crate::matrix::ObjectLock::acquire(
      cimpl,
      &self.region,
      &self.uuid,
      0
    ).await;
    
    let epoch2 = tsgen::get_epoch();

    if(epoch != epoch2) {
      return;
    }

  }

  fn signalbuffer(&mut self) -> String {
    let crate::tsgen::TsEpochPair {ts, epoch} = crate::tsgen::get_ts_pair();
    let s = self.cursor.advance();
    epoch + &ts + &s
  }

  
}


pub struct Node<I: INode> {
  /*
  W: window Type 
  I: Node Interface,
  V: value (u16, u32, u64)
  */
  pub uuid: String,
  pub host: String, 

  pub seed: String,

  pub mtx_session: crate::matrix::ObjectSession,
  node_impl: I,
}


