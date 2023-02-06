use aws_sdk_dynamodb::{ Client, model::AttributeValue, };
use sha2::digest::block_buffer::Block;
use crate::block::BlockBuffer;
use crate::dynamoc::{self, SelectQuerySetResult};
use crate::dynamoc::DynamoSelectQueryContext;
use crate::event::{EventBuffer, Event};
use crate::node;

use crate::primitives;

use crate::primitives::{DataType, dunwrap_s};
use crate::tsgen;
use std::collections::{HashMap};
use hex_literal::hex;
use sha2::{Sha256, Sha512, Digest};

use log::{error, info, debug};
use async_trait::async_trait;

use crate::cursor::{ TCursor, Cursor};

#[async_trait]
pub trait BlockFinalizable<T> {

  fn reduce_records(
    &self,
    records: &Vec<HashMap<String, DataType>>,
  ) -> String;

  fn build_block(
    &self,
    epoch: &std::string::String,
    prev_blockhash: &std::string::String,
    records: &Vec<HashMap<String, DataType>>,
    finalized_at: &std::string::String,
  ) -> crate::block::BlockBuffer;

  async fn get_block(&self, epoch: &std::string::String) -> HashMap<String, DataType>;

}

#[async_trait]
pub trait BlockVerifiable<T> {
  fn verify_block(
    &self,
    hm_block: &HashMap<String, DataType>,
    events: &Vec<HashMap<String, DataType>>,
  ) -> bool;

  async fn _get(&self, blockhash: &std::string::String);
}


#[async_trait]
impl <Event> BlockVerifiable<Event> for FortunateEventFinalizer {

  async fn _get(
    &self,
    blockhash: &std::string::String
  ) {
    let mut cursor = crate::cursor::Cursor::new(blockhash);

    let epoch = cursor.epoch();
    let ts = cursor.timestamp();
    let block_hash = cursor.advance(92);

    println!("{} {} {}", epoch, ts, block_hash);
  }

  fn verify_block(
    &self,
    hm_block: &HashMap<String, DataType>,
    events: &Vec<HashMap<String, DataType>>,
  ) -> bool {
    let s: &dyn BlockFinalizable<Event> = self;

    let epoch = dunwrap_s(
      hm_block.get("epoch").unwrap()
    );

    let ts = dunwrap_s(
      hm_block.get("ts").unwrap()
    );

    let prev_blockhash = dunwrap_s(
      hm_block.get("prev_blockhash").unwrap()
    );

    let _block 
      = s.build_block(&epoch, &prev_blockhash, &events, &ts);
    let computed = self.hash_eventblock(&_block);

    let origin = crate::block::BlockHash { 
      hash: dunwrap_s(hm_block.get("hash").unwrap())
    };

    computed == origin
  }
}

const BlockFinalizableEventLogger: crate::flog::FortunateLogger  = crate::flog::FortunateLogger::new("blockfinalizer<event>");

#[async_trait]
impl <Event> BlockFinalizable<Event> for FortunateEventFinalizer {

  fn reduce_records(
    &self,
    events: &Vec<HashMap<String, DataType>>,
  ) -> String {
    let mut ret = String::from("");

    for e in events.iter() {
      let s = dunwrap_s(
        e.get("buffer").unwrap()
      );

      BlockFinalizableEventLogger.info(
        format!(
        "reduce records; event buffer={};", s
        ).as_str()
      );
      
      ret += &s;
    };

    ret
  }

  fn build_block(
    &self,
    epoch: &std::string::String,
    prev_blockhash: &std::string::String,
    records: &Vec<HashMap<String, DataType>>,
    finalized_at: &std::string::String
  ) -> crate::block::BlockBuffer {
    let buffer = String::from(epoch);
    let s: &dyn BlockFinalizable<Event> = self;

    let event_block_buffer = s.reduce_records(records);

    crate::block::BlockBuffer { buffer: buffer + prev_blockhash + finalized_at + &event_block_buffer }
  }

  async fn get_block(
    &self,
    epoch: &std::string::String,
  ) -> HashMap<String, DataType> {

    let qctx = DynamoSelectQueryContext {
      table_name: &"event_blocks",
      conditions: Some(vec![
        crate::primitives::Pair::<&'static str, crate::primitives::DataType> {
            k: "epoch",
            v: crate::primitives::DataType::S(epoch.to_owned())
        },
      ]),
      query_subtype: dynamoc::DynamoSelectQuerySubType::One
    };

    let items = self._event_dimpl.q(
      &self.dynamo_client,
      &qctx 
    ).await.unwrap();

    match items {
      dynamoc::SelectQuerySetResult::One(x) 
        => x.unwrap(), 
      dynamoc::SelectQuerySetResult::All(x) 
        => panic!("get block used invalid query sub type.")
    }
  }
}

pub struct FortunateEventFinalizer {
  _event_dimpl: dynamoc::DynamoHandler,
  _eventblock_dimpl: dynamoc::DynamoHandler,

  dynamo_client: Client,
  region: std::string::String,
}

impl FortunateEventFinalizer {
  pub async fn new(region: &std::string::String) -> Self {
    FortunateEventFinalizer {
      _event_dimpl: dynamoc::DynamoHandler::event(),
      _eventblock_dimpl: dynamoc::DynamoHandler::eventblock(),
      dynamo_client: dynamoc::get_dynamo_client().await,
      region: region.to_owned()
    }
  }

  pub async fn get_events(
    &self, 
    epoch: &String
  ) -> Result<Vec<HashMap<String, DataType>>, ()> {
    let qctx = DynamoSelectQueryContext {
        table_name: &"events",
        conditions: Some(vec![
                crate::primitives::Pair::<&'static str, crate::primitives::DataType> {
                    k: "epoch",
                    v: crate::primitives::DataType::S(epoch.to_owned())
                },
        ]),
        query_subtype: crate::dynamoc::DynamoSelectQuerySubType::All
    };

    let result = 
      self._event_dimpl.q(&self.dynamo_client, &qctx).await.expect("dynamo select operation is failed.");

    match result {
        dynamoc::SelectQuerySetResult::All(x) => Ok(x.unwrap()),
        _ => panic!("get events uses invalid query sub type.")
    }

  }

  pub fn hash_eventblock(
    &self, 
    blockbuffer: &BlockBuffer
  ) -> crate::block::BlockHash {
    let mut h = Sha256::new();
    h.update(String::from(blockbuffer.buffer.to_owned()));
    crate::block::BlockHash { hash: format!("{:X}", h.finalize()) }
  }
  
  pub async fn commit_block(
    &self,
    epoch: &String, 
    block: &crate::block::BlockBuffer,
    ts: &String, 
    prev_blockhash: &String
  ) -> Result<crate::block::BlockHash, aws_sdk_dynamodb::Error> {
    let mut data = HashMap::<String,String>::new();
    let blockhash = self.hash_eventblock(&block);

    data.insert(String::from("epoch"), epoch.to_owned());
    data.insert(String::from("region"), self.region.to_owned());
    data.insert(String::from("hash"), blockhash.hash.to_owned());
    data.insert(String::from("block"), block.buffer.to_owned());
    data.insert(String::from("ts"), ts.to_owned());
    data.insert(String::from("prev_blockhash"), prev_blockhash.to_owned());
    data.insert(String::from("next_blockhash"), String::from(""));
    data.insert(String::from("finalized"), String::from("y"));

    let request = self._eventblock_dimpl.make_insert_request(&self.dynamo_client, data);
    self._event_dimpl.commit(request).await
      .expect("dynamo operation failed some reason");

    Ok(blockhash)
  }

  pub async fn finalize_eventblock(
    &self,
    epoch: &std::string::String, 
    prev_epoch: Option<&String>,
  ) -> std::option::Option<crate::block::BlockHash> {
    let mut _prev_block_hash = String::from("");
    let mut _prev_epoch = None;
    let s: &dyn BlockFinalizable<Event> = self;

    match prev_epoch {
      Some(x) => { 
        _prev_epoch = Some(prev_epoch.unwrap());
        _prev_block_hash = dunwrap_s(s.get_block(&(prev_epoch.unwrap())).await.get("hash").unwrap());
      },
      None => {
        _prev_epoch = Some(&String::from("GENESS"));
        _prev_block_hash = String::from("GENESS");
      }
    }

    let events = self.get_events(epoch).await.expect("get event failed.");
    let finalized_at = tsgen::get_ts();
    let s: &dyn BlockFinalizable<Event> = self;

    let blockbuffer
      = s.build_block(epoch, &_prev_block_hash,&events, &finalized_at);

    self.commit_block(
      &epoch, 
      &blockbuffer, 
      &finalized_at, 
      &_prev_block_hash
    ).await.expect("commit block failed some reason...");

    let blockhash = self.hash_eventblock(&blockbuffer);
    Some(blockhash)

  }

  pub async fn verify_event_payload(
    &self, 
    epoch: &std::string::String,
    event_key: &std::string::String,
    payload: &std::string::String,
  ) -> bool {

    /**
    let rspn = self.get_event

    match rspn {
      Ok(result) => {

        match &result {
          Some(event) =>  {
            let event_key = event.get("event_key").unwrap().as_s().unwrap();
            let _payload = EventBuffer::get_payload_header_s(event_key);
            let hashed_payload = crate::hashlib::hash_payload(&payload);
          }, 
          None => {
            error!("There is no event with key {}", &event_key);
            return false;
          }
        }

      },
      Err(e) => {
        error!("{:?}", e);
      }

    }
    */
    true
  }
}    

pub struct FortunateNodeSignalFinalizer {
  pub uuid: std::string::String,

  _nodesignal_dimpl: dynamoc::DynamoHandler,
  _nodesignalblock_dimpl: dynamoc::DynamoHandler,

  dynamo_client: Client,

  pub cimpl: crate::sessions::RedisImpl,

  pub region: std::string::String,
  pub logger: crate::flog::FortunateLogger,
}

impl FortunateNodeSignalFinalizer {
  pub async fn new(
    region: &std::string::String,
  ) -> Self {
    let _uuid = format!("nodesignalfinalizer:{}", region);
    
    let mut fnz = FortunateNodeSignalFinalizer {
      uuid: _uuid.to_owned(),
      _nodesignal_dimpl: dynamoc::DynamoHandler::nodesignal(), // TODO
      _nodesignalblock_dimpl: dynamoc::DynamoHandler::nodesignalblock(), // TODO
      dynamo_client: dynamoc::get_dynamo_client().await,

      cimpl: crate::sessions::RedisImpl::new(Some(_uuid.to_owned())),

      region: region.to_owned(),
      logger: crate::flog::FortunateLogger::new("nodesignalfinalizer"),
    };

    crate::matrix::ObjectLock::init_object_lock(&mut fnz.cimpl, &fnz.uuid);

    fnz
  }

  pub async fn get_node_signals(&self, epoch: &String) -> Vec<HashMap<String, DataType>> {
    let qctx = DynamoSelectQueryContext {
      table_name: &"node_signals",
      conditions: Some(vec! [
        primitives::Pair::<&'static str, primitives::DataType> {
            k: "epoch",
            v: primitives::DataType::S(epoch.to_owned()),
        },
       ]),
      query_subtype: dynamoc::DynamoSelectQuerySubType::All
    };

    let items = 
      self._nodesignal_dimpl
          .q(
              &self.dynamo_client, 
              &qctx
            ).await.unwrap();

    match items {
      dynamoc::SelectQuerySetResult::All(x) => x.unwrap(),
      _ => panic!("")
    }
  }

  pub async fn commit_nodesignalblock(
    &self, 
    epoch: &String, 
    block: &crate::block::BlockBuffer, 
    ts: &String,
    prev_blockhash: &String,
  ) 
  -> Result<crate::block::BlockHash, aws_sdk_dynamodb::Error> {
    let mut data = HashMap::<String,String>::new();
    let blockhash = self.hash_nodesignalblock(&block);

    data.insert(String::from("epoch"), epoch.to_owned());
    data.insert(String::from("region"), self.region.to_owned());
    data.insert(String::from("hash"), blockhash.hash.to_owned());
    data.insert(String::from("block"), block.buffer.to_owned());
    data.insert(String::from("ts"), ts.to_owned());
    data.insert(String::from("prev_blockhash"), prev_blockhash.to_owned());
    data.insert(String::from("next_blockhash"), String::from(""));
    data.insert(String::from("finalized"), String::from("y"));

    let request = self._nodesignalblock_dimpl.make_insert_request(&self.dynamo_client, data);

    self._nodesignalblock_dimpl.commit(request).await
      .expect("dynamo operation failed some reason.");

    Ok(blockhash)
  }

  pub async fn verify_nodesignalblock(&self, epoch: &String) -> bool {
    let block = self.get_block(epoch).await;
    let signals = self.get_node_signals(epoch).await;

    let ts = dunwrap_s(block.get("ts").unwrap());
    let prev_blockhash = dunwrap_s(block.get("prev_blockhash").unwrap());

    let _block = self.build_block(epoch, &prev_blockhash, &signals, &ts);
    let computed = self.hash_nodesignalblock(&_block);
    
    let origin = crate::block::BlockHash { hash: dunwrap_s(block.get("hash").unwrap()) };

    computed == origin
  }

  pub fn hash_nodesignalblock(
    &self, 
    blockbuffer: &BlockBuffer
  ) -> crate::block::BlockHash {
    let mut h = Sha256::new();
    h.update(String::from(blockbuffer.buffer.to_owned()));
    crate::block::BlockHash { hash: format!("{:X}", h.finalize()) }
  }

  fn reduce_signals(&self, signals: &Vec<HashMap<String, DataType>>) -> String {
    let mut ret = String::from("");

    for sig in signals.iter() {
      let data = sig.get("data").unwrap();
      ret += dunwrap_s(data).as_str();
    }

    ret
  }

  pub async fn finalize_nodesignalblock(
    &self, 
    epoch: &String, 
    prev_epoch: Option<&String>
  ) -> std::option::Option<crate::block::BlockHash> {
    let mut _prev_block_hash = String::from("");
    let mut _prev_epoch = None;

    match prev_epoch {
      Some(x) => { 
        _prev_epoch = Some(prev_epoch.unwrap());
        _prev_block_hash = Some(self.get_block_hash(&(prev_epoch.unwrap())).await).unwrap();
      },
      None => {
        _prev_epoch = Some(&String::from("GENESS"));
        _prev_block_hash = String::from("GENESS");
      }
    }

    let signals = self.get_node_signals(epoch).await;
    let finalized_at = tsgen::get_ts();

    let blockbuffer= self.build_block(epoch, &_prev_block_hash,&signals, &finalized_at);
    let result = self.commit_nodesignalblock(
      &epoch, 
      &blockbuffer, 
      &finalized_at, 
      &_prev_block_hash
    ).await;

    match result {
      Err(e)=> {
        error!("commit failed. {:?}", e)
      },
      Ok(x) => {

      }
    }
    let blockhash = self.hash_nodesignalblock(&blockbuffer) ;
    Some(blockhash)
  }

  pub fn build_block(
    &self, 
    epoch: &String,
    prev_blockhash: &String,
    signals: &Vec<HashMap<String, DataType>>,
    finalized_at: &String,    
  ) -> crate::block::BlockBuffer {
    let buffer = String::from(epoch);
    let signal_buffer_block = &self.reduce_signals(signals);

    crate::block::BlockBuffer { buffer: buffer + prev_blockhash + &finalized_at + &signal_buffer_block }
  }

  pub async fn get_block(&self, epoch: &String) -> HashMap<String, DataType> {
    let qctx = DynamoSelectQueryContext {
      table_name: &"node_signal_blocks",
      conditions: Some(vec![
              crate::primitives::Pair::<&'static str, crate::primitives::DataType> {
                  k: "epoch",
                  v: crate::primitives::DataType::S(epoch.to_owned())
              },
      ]),
      query_subtype: dynamoc::DynamoSelectQuerySubType::One
    };

    let items = self._nodesignal_dimpl.q(
      &self.dynamo_client,
      &qctx 
    ).await.unwrap();

    match items {
      dynamoc::SelectQuerySetResult::One(x) 
        => x.unwrap(), 
      dynamoc::SelectQuerySetResult::All(x) 
        => panic!("get block used invalid query sub type.")
    }
  }

  pub async fn get_block_hash(&self, epoch: &String) -> String {
    let res = self.get_block(epoch).await;
    dunwrap_s(res.get("hash").unwrap())
  }


}