use std::collections::HashMap;
use std::fmt::Error;
use std::ops::Deref;
use rand::Rng;

use crate::cursor;
use crate::dynamoc;
use crate::flog::FortunateLogger;
use crate::matrix;
use crate::matrix::MatrixComponent;
use crate::node::{ NodeSignalBase, NodeSignalKey };
use crate::primitives::dunwrap_s;
use crate::sessions::RedisImpl;
use crate::tsgen;
use crate::node;
use crate::cursor::{ Cursor };
use log::{debug, error, info, trace, warn};
use async_trait::async_trait;

use aws_sdk_dynamodb::{ Client, };

#[derive(std::clone::Clone, Debug)]
pub enum EventType {
  PE(String)
}


#[derive(std::clone::Clone, Debug)]
pub enum EventSubType {

}

#[derive(std::clone::Clone, Debug)]
pub enum EventResult {
  TF(bool),
  RAGNE(u64),
}


#[derive(std::clone::Clone, Debug)]
pub struct Event {
  pub event_key: std::string::String,
  pub epoch: std::string::String,
  pub event_type: EventType,
  pub event_subtype: std::option::Option<EventSubType>,
  pub ts: tsgen::Timestamp,

  pub ref_signals: std::option::Option<Vec<String>>,
  pub result: std::option::Option<EventResult>,

  pub buffer: std::option::Option<EventBuffer>,
  pub payload: Option<std::string::String>,
}


impl Event {

  pub fn new(
      epoch: &String,
      event_key: &std::string::String,
      event_type: &EventType,   
      buffer: &EventBuffer,
      result: std::option::Option<EventResult>,
      payload: std::option::Option<std::string::String>,
  ) -> Self {
    let ts = tsgen::get_ts();

    Event {
      event_key: event_key.to_owned(),
      epoch: epoch.to_owned(),
      event_type: event_type.to_owned(),
      ts: tsgen::Timestamp::from_str(&ts),
      event_subtype: None,
      ref_signals: None,
      buffer: Some(buffer.to_owned()),
      result: result,
      payload:payload,
    }
  }


  pub fn window(&self, w: &mut HashMap<std::string::String, std::string::String>) {
    w.insert(String::from("epoch"), self.epoch.to_owned());
    w.insert(String::from("event_key"), self.event_key.to_owned());
    w.insert(String::from("buffer"), self.buffer.as_ref().unwrap().buffer.to_owned());
    w.insert(String::from("ts"), self.ts.to_owned().s);

    match &self.payload {
      Some(x) => {
        w.insert(String::from("payload"), x.to_owned());
      },
      None => {
        w.insert(String::from("payload"), String::from(""));
      }

    }

  }

  pub fn parse_event_buffer(event_buffer: &EventBuffer) -> Event {
    /**
     * Eventbuffer to Event
     */
    let mut cur = cursor::Cursor::new(&event_buffer.buffer);

    let epoch = cur.advance(6);
    let ts = cur.advance(16);
    let payload_header = cur.advance(16);

    let event_key = epoch.to_owned() + &ts + &payload_header;

    let event_type = cur.advance(6);
    let ref_signal_count = cur.advance(4).parse::<u32>().unwrap();

    let signal_buffer = cur.rest();
    let mut ref_signals = Vec::<String>::new();

    let mut _cur = cursor::Cursor::new(&signal_buffer);

    for _ in 0..ref_signal_count {
      let s = _cur.advance(22);
      let k = node::NodeSignalBase::parse_key(&s);
      ref_signals.push(k.signal_key);
    }

    Event {
      event_key: event_key,
      epoch: epoch,
      event_type: EventType::PE(event_type),
      event_subtype: None,
      ts: tsgen::Timestamp::from_str(&ts), 

      ref_signals: Some(ref_signals),
      result: None,
      buffer: Some(event_buffer.to_owned()),

      payload: None, // Event from buffer does not have this value.
    }
  }

}

#[derive(std::clone::Clone, Debug)]
pub struct EventBuffer {
  pub event_key: std::string::String,
  pub buffer: std::string::String,
}

impl EventBuffer {
  pub fn get_payload_header(eb: &EventBuffer) -> std::string::String {
    let mut cursor = cursor::Cursor::new(&eb.buffer);
    cursor.advance(6 + 16);
    cursor.advance(16)
  }

  pub fn get_payload_header_s(event_key: &std::string::String) -> std::string::String {
    let mut cursor = cursor::Cursor::new(event_key);
    cursor.advance(6 + 16);
    cursor.advance(16)
  }
}

pub struct EventCmtr {
  dynamo_client: Client,
  dquery_handler: dynamoc::DynamoHandler,
}

impl EventCmtr {

  pub async fn new() -> Self {
    EventCmtr {
      dynamo_client: dynamoc::get_dynamo_client().await,
      dquery_handler: dynamoc::DynamoHandler::event(),
    }
  }

  pub async fn commit_event(&self, event: &Event) -> Result<(), Error> {
    let mut data = HashMap::<String, String>::new();
    event.window(&mut data);

    let request = self.dquery_handler.make_insert_request(&self.dynamo_client, data);
    let rspn = self.dquery_handler.commit(request).await;

    match rspn {
      Ok(()) => {
        info!("event {:?} successfully registered!", event)
      },
      Err(e) => {
        info!("{:?}", e);
      }
    }

    Ok(())
  }
}

pub struct PEventGenerator {
  pub uuid: std::string::String, 
  pub ts: std::string::String,

  pub dynamo_client: aws_sdk_dynamodb::Client,
  pub cimpl: RedisImpl,

  pub seed: usize,
  pub logger: FortunateLogger,
  pub region: String,
}

#[async_trait]
impl matrix::MatrixComponent for PEventGenerator {

  async fn matrix_lock_acquire(
      &mut self,
    ) -> bool {

    self.logger.info(
      format!("Acquire for {} from {}; mod:{:?}", &self.region, &self.uuid, 0)
            .as_str()
    );

    crate::matrix::ObjectLock::acquire(
      &mut self.cimpl,
      &self.region,
      &self.uuid,
      0,
    ).await
  }

} 

impl PEventGenerator {

  pub async fn new(region: &std::string::String) -> PEventGenerator {
    let ts =  tsgen::get_ts();
    let uuid = String::from("PEVGEN#") + &ts;

    return PEventGenerator {
      uuid: uuid, 
      seed: 0,
      ts: ts,
      dynamo_client: crate::dynamoc::get_dynamo_client().await,
      cimpl: crate::sessions::RedisImpl::new(Some("peventgenerator".to_string())),
      logger: FortunateLogger::new(
        std::string::String::from("PEventGenerator")
      ),
      region: region.to_owned()
    }
  }

  pub async fn get_node_signalkeys(&self, epoch: &String) -> 
    Result<std::vec::Vec<node::NodeSignalKey>, ()> {
    let signals = 
      node::FNode::get_node_s_signals(&self.dynamo_client, epoch).await;

    let _signals: Vec<node::NodeSignalKey> = signals.iter()
      .map(|x| dunwrap_s(x.get("data").unwrap()) )
      .map(|x| node::NodeSignalBase::parse_key(&x))
      .collect();

    if (_signals.len() > 0) {
      Ok(_signals)
    }
    else {
      Err(())
    }
  }

  pub async fn get_random_node_signals<'a>(&self, signals: &'a Vec<NodeSignalKey>, signal_num: usize, result: &mut Vec<&'a NodeSignalKey>) {

    let max = signals.len() - 1;

    for _ in 0..signal_num {
      let idx = crate::algorithms::get_randnum_r(0, u32::try_from(max).unwrap());
      result.push(&signals[usize::try_from(idx).unwrap()]);
    }

  }


  pub async fn generate_event_pe2(
    &mut self,
    payload: &std::string::String,
  ) -> std::result::Result<Event, Error> {

    self.matrix_lock_acquire().await;
    let epoch = 
      matrix::Matrix::get_prev_epoch(&self.region, &mut self.cimpl, &self.uuid).await;

    let signals = self.get_node_signalkeys(&epoch).await;
    let mut ref_signals = std::vec::Vec::<&NodeSignalKey>::new();

    let _fn = 
      |arr: &Vec<&NodeSignalKey>| arr.iter().fold(true, |x: bool, y: &&NodeSignalKey| x && true ); //TODO

    match &signals {
      Ok(x) => {
        self.get_random_node_signals(x, 2, &mut ref_signals).await;
      },
      _ => panic!("there is no node signals.")
    };

    self.generate_event_from_signals(
      &epoch,
      payload,
      &EventType::PE(String::from("000001")),
      &ref_signals,
      &_fn
    )

  }

  pub fn generate_event_from_signals(
    &self,
    epoch: &std::string::String,
    payload: &std::string::String,
    event_type: &EventType,
    ref_signals: &std::vec::Vec<&NodeSignalKey>,
    signal_lambda: &dyn Fn(&Vec<&NodeSignalKey>) -> bool,
  ) -> std::result::Result<Event, Error> {

    let ts = tsgen::get_ts_c();

    let buffer = 
      PEventGenerator::build_event_buffer(
        &event_type, 
        epoch, 
        payload,
        &ts, 
        ref_signals
      ).unwrap();

    let r = signal_lambda(&ref_signals);

    Ok(Event::new(
      &epoch, 
      &buffer.event_key, 
      &event_type, 
      &buffer, 
      Some(EventResult::TF(r)), 
      Some(payload.to_owned())
    ))
  }

  pub fn build_event_buffer(
    event_type: &EventType,
    epoch: &std::string::String,
    payload: &std::string::String,
    ts: &tsgen::Timestamp,
    ref_signals: &std::vec::Vec<&crate::node::NodeSignalKey>,
  ) -> std::result::Result<EventBuffer, Error> {

    let mut buffer = epoch.to_owned() + &ts.s;
    buffer += &crate::hashlib::hash_payload(payload);

    let event_key = buffer.to_owned();

    match event_type {
      EventType::PE(x) => {
        buffer += &x;
      },
      _ => {

      }
    }

    let ref_signal_count = format!("{:04}", ref_signals.len());
    buffer += &ref_signal_count;

    for s in ref_signals.iter() {
      buffer += &s.signal_key;
    }

    Ok(EventBuffer { event_key: event_key, buffer: buffer })
  }

}