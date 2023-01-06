use aws_sdk_dynamodb::{ Client, model::AttributeValue, };
use sha2::digest::block_buffer::Block;
use crate::block::BlockBuffer;
use crate::dynamoc;
use crate::node;
use crate::tsgen;
use std::collections::{HashMap};
use hex_literal::hex;
use sha2::{Sha256, Sha512, Digest};


pub struct FortunateEventFinalizer {
  _event_dimpl: dynamoc::DynamoHandler,
  dynamo_client: Client,
}

impl FortunateEventFinalizer {
  pub async fn new() -> Self {
    FortunateEventFinalizer {
      _event_dimpl: dynamoc::DynamoHandler::event(),
      dynamo_client: dynamoc::get_dynamo_client().await,
    }
  }

  pub async fn get_events(&self, epoch: &String) -> Vec<HashMap<String, AttributeValue>> {
    let mut q = HashMap::<String, String>::new();

    q.insert(String::from("table_name"), String::from("events"));
    q.insert(String::from("key_column"), String::from("epoch"));
    q.insert(String::from("key_value"), epoch.to_owned());

    let items = self._event_dimpl.query(&self.dynamo_client, q).await.unwrap();
    let items_u = items.unwrap();

    for v in items_u.iter() {
      let data = v.get(&String::from("data"));
    }

    items_u
  }
}

pub struct FortunateNodeSignalFinalizer {
  _nodesignal_dimpl: dynamoc::DynamoHandler,
  _nodesignalblock_dimpl: dynamoc::DynamoHandler,

  dynamo_client: Client,
}

impl FortunateNodeSignalFinalizer {
  pub async fn new() -> Self {
    
    FortunateNodeSignalFinalizer {
      _nodesignal_dimpl: dynamoc::DynamoHandler::nodesignal(), // TODO
      _nodesignalblock_dimpl: dynamoc::DynamoHandler::nodesignalblock(), // TODO
      dynamo_client: dynamoc::get_dynamo_client().await,
    }
  }

  pub async fn get_node_signals(&self, epoch: &String) -> Vec<HashMap<String, AttributeValue>> {
    let mut q = HashMap::<String,String>::new();

    q.insert(String::from("table_name"), String::from("node_signals"));
    q.insert(String::from("key_column"), String::from("epoch"));
    q.insert(String::from("query_value"), epoch.to_owned());

    let items = self._nodesignal_dimpl.query(&self.dynamo_client, q).await.unwrap();

    let items_u = items.unwrap();

    for v in items_u.iter() {
      let data = v.get(&String::from("data"));
    }

    items_u
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
    data.insert(String::from("group"), String::from("region-01"));
    data.insert(String::from("hash"), blockhash.hash.to_owned());
    data.insert(String::from("block"), block.buffer.to_owned());
    data.insert(String::from("ts"), ts.to_owned());
    data.insert(String::from("prev_blockhash"), prev_blockhash.to_owned());
    data.insert(String::from("next_blockhash"), String::from(""));
    data.insert(String::from("finalized"), String::from("y"));

    let request = self._nodesignalblock_dimpl.make_insert_request(&self.dynamo_client, data);
    self._nodesignalblock_dimpl.commit(request).await;
    Ok(blockhash)
  }

  pub async fn verify_nodesignalblock(&self, epoch: &String) -> bool {
    let mut query = HashMap::<String, String>::new();

    query.insert(String::from("table_name"), String::from("node_signal_blocks")); //TODO 일음바꾸기
    query.insert(String::from("key_column"), String::from("epoch")); 
    query.insert(String::from("query_value"), String::from(epoch)); 

    let block = self.get_block(epoch).await;
    let signals = self.get_node_signals(epoch).await;

    let ts = block.get("ts").unwrap().as_s().unwrap();
    let prev_blockhash = block.get("prev_blockhash").unwrap().as_s().unwrap();

    let _block = self.build_block(epoch, &prev_blockhash, &signals, ts);
    let computed = self.hash_nodesignalblock(&_block);
    
    let origin = crate::block::BlockHash { hash: block.get("hash").unwrap().as_s().unwrap().to_owned() };

    computed == origin
  }

  pub fn hash_nodesignalblock(&self, blockbuffer: &BlockBuffer) -> crate::block::BlockHash {
    let mut h = Sha256::new();

    h.update(String::from(blockbuffer.buffer.to_owned()));

    crate::block::BlockHash { hash: format!("{:X}", h.finalize()) }
  }

  fn reduce_signals(&self, signals: &Vec<HashMap<String, AttributeValue>>) -> String {
    let mut ret = String::from("");

    for sig in signals.iter() {
      let data = sig.get("data").unwrap();
      ret += data.as_s().unwrap();
    }

    ret
  }

  pub async fn finalize_nodesignalblock(&self, epoch: &String, prev_epoch: Option<&String>) -> std::option::Option<crate::block::BlockHash> {
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
        println!("commit failed. {:?}", e)
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
    signals: &Vec<HashMap<String, AttributeValue>>,
    finalized_at: &String,    
  ) -> crate::block::BlockBuffer {
    let buffer = String::from(epoch);
    let signal_buffer_block = &self.reduce_signals(signals);

    crate::block::BlockBuffer { buffer: buffer + prev_blockhash + &finalized_at + &signal_buffer_block }
  }

  pub async fn get_block(&self, epoch: &String) -> HashMap<String, AttributeValue> {
    let mut data = HashMap::<String, String>::new();

    data.insert(String::from("table_name"), String::from("node_signal_blocks")); //TODO
    data.insert(String::from("key_column"), String::from("epoch"));
    data.insert(String::from("query_value"), epoch.to_owned());
    println!("{:?}", data);

    let res = self._nodesignal_dimpl.query(
      &self.dynamo_client,
      data
    ).await;

    res.unwrap().unwrap()[0].to_owned()
  }

  pub async fn get_block_hash(&self, epoch: &String) -> String {
    let res = self.get_block(epoch).await;
    res.get("hash").unwrap().as_s().unwrap().to_owned()
  }


}