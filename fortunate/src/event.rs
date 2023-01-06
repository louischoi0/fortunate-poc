use std::collections::HashMap;
use std::fmt::Error;
use std::ops::Deref;
use rand::Rng;

use crate::cursor;
use crate::dynamoc;
use crate::node::NodeSignal;
use crate::node::NodeSignalKey;
use crate::tsgen;
use crate::node;
use crate::cursor::{ Cursor };

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

  pub ref_signals: std::option::Option<Vec<NodeSignalKey>>,
  pub result: std::option::Option<EventResult>,

  pub buffer: std::option::Option<EventBuffer>,
}

impl Event {

  pub fn new(
      epoch: &String,
      event_type: &EventType,   
      buffer: &EventBuffer,
      result: std::option::Option<EventResult>,
  ) -> Self {
    let ts = tsgen::get_ts();
    let event_key = epoch.to_owned() + &ts;

    Event {
      event_key: event_key,
      epoch: epoch.to_owned(),
      event_type: event_type.to_owned(),
      ts: tsgen::Timestamp::from_str(&ts),
      event_subtype: None,
      ref_signals: None,
      buffer: Some(buffer.to_owned()),
      result: result,
    }

  }
  pub fn window(&self, w: &mut HashMap<std::string::String, std::string::String>) {
    w.insert(String::from("epoch"), self.epoch.to_owned());
    w.insert(String::from("event_key"), self.event_key.to_owned());
    w.insert(String::from("buffer"), self.buffer.as_ref().unwrap().buffer.to_owned());
    w.insert(String::from("ts"), self.ts.to_owned().s);
  }

  pub fn parse_event_buffer(event_buffer: &EventBuffer) -> Event {
    let mut cur = cursor::Cursor::new(event_buffer.buffer.to_owned());

    let epoch = cur.advance(5);
    let ts = cur.advance(16);
    let event_key = epoch.to_owned() + &ts;

    let event_type = cur.advance(6);
    let ref_signal_count = cur.advance(4).parse::<u32>().unwrap();

    let signal_buffer = cur.rest();
    let mut ref_signals = Vec::<String>::new();

    let mut _cur = cursor::Cursor::new(signal_buffer);
    
    for _ in 0..ref_signal_count {
      //ref_signals.push(_cur.advance(31));
    }

    Event {
      event_key: event_key,
      epoch: epoch,
      event_type: EventType::PE(event_type),
      event_subtype: None,
      ts: tsgen::Timestamp::from_str(&ts), 

      ref_signals: None,
      result: None,
      buffer: Some(event_buffer.to_owned()),
    }
  }

}

#[derive(std::clone::Clone, Debug)]
pub struct EventBuffer {
  pub buffer: std::string::String,
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
        println!("event {:?} successfully registered!", event)
      },
      Err(e) => {
        println!("{:?}", e);
      }
    }

    Ok(())
  }
}

#[derive(std::clone::Clone)]
pub struct PEventGenerator {
  pub uuid: std::string::String, 
  pub ts: std::string::String,

  pub dynamo_client: aws_sdk_dynamodb::Client,
  pub seed: usize,
}

impl PEventGenerator {

  pub async fn new() -> PEventGenerator {
    let ts =  tsgen::get_ts();
    let uuid = String::from("PEVGEN#") + &ts;

    return PEventGenerator {
      uuid: uuid, 
      seed: 0,
      ts: ts,
      dynamo_client: crate::dynamoc::get_dynamo_client().await
    }
  }

  pub async fn get_node_signalkeys(&self, epoch: &String) -> std::vec::Vec<node::NodeSignalKey> {
    let signals = 
      node::FNode::get_node_s_signals(&self.dynamo_client, epoch).await;

    signals.iter()
      .map(|x| x.get("data").unwrap().as_s().unwrap().to_owned() )
      .map(|x| node::NodeSignal::parse_key(&x))
      .collect()
  }

  pub async fn get_random_node_signals<'a>(&self, signals: &'a Vec<NodeSignalKey>, signal_num: usize, result: &mut Vec<&'a NodeSignalKey>) {
    println!("{:?}", signals);

    let max = signals.len() - 1;

    for _ in 0..signal_num {
      let idx = crate::algorithms::get_randnum_r(0, u32::try_from(max).unwrap());
      result.push(&signals[usize::try_from(idx).unwrap()]);
    }

  }

  pub async fn generate_event_pe2(
    &self,
    epoch: &String
  ) -> std::result::Result<Event, Error> {

    let signals = self.get_node_signalkeys(epoch).await;
    let mut ref_signals = std::vec::Vec::<&NodeSignalKey>::new();

    self.get_random_node_signals(&signals, 2, &mut ref_signals).await;
    println!("{:?}", ref_signals);

    let _fn = 
      |arr: &Vec<&NodeSignalKey>| arr.iter().fold(true, |x: bool, y: &&NodeSignalKey| x && y.deref().at(0));

    self.generate_event_from_signals(
      &EventType::PE(String::from("000001")),
      epoch,
      &ref_signals,
      &_fn
    )
  }

  pub fn generate_event_from_signals(
    &self,
    event_type: &EventType,
    epoch: &std::string::String,
    ref_signals: &std::vec::Vec<&NodeSignalKey>,
    signal_lambda: &dyn Fn(&Vec<&NodeSignalKey>) -> bool,
  ) -> std::result::Result<Event, Error> {

    let ts = tsgen::get_ts_c();

    let buffer = PEventGenerator::build_event_buffer(&event_type, epoch, &ts, ref_signals).unwrap();

    let r = signal_lambda(&ref_signals);
    Ok(Event::new(&epoch, &event_type, &buffer, Some(EventResult::TF(r))))
  }

  pub fn build_event_buffer(
    event_type: &EventType,
    epoch: &std::string::String,
    ts: &tsgen::Timestamp,
    ref_signals: &std::vec::Vec<&crate::node::NodeSignalKey>,
  ) -> std::result::Result<EventBuffer, Error> {
    let mut buffer = epoch.to_owned() + &ts.s;

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

    Ok(EventBuffer { buffer: buffer })
  }

}