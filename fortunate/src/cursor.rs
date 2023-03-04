use std::{collections::HashMap};
use crate::{window::{BitWindow, TBitWindow}, fnode::BitArraySignalKey, flog::FortunateLogger, primitives::TString, node::NodeSignalKeyRefSer};
use crate::event::Event;

const CursorLogger: FortunateLogger = FortunateLogger::new("cursor");

#[derive(Debug)]
pub struct Cursor {
  pub msg: String,
  pub now: usize,
}

pub trait TCursor<T> {
  fn advance(&mut self, size: usize) -> T;
  fn read(&self, size: usize) -> T;
}

impl TCursor<String> for Cursor {
  fn advance(&mut self, size: usize) -> String {
    let s = String::from(&self.msg[self.now..self.now+size]);
    self.now += size;
    s
  }

  fn read(&self, size: usize) -> String {
    let s = String::from(&self.msg[self.now..self.now+size]);
    s 
  }

}

impl Cursor {
  pub fn eof(&self) -> bool {
    self.now > (self.msg.len() - 1)
  }

  pub fn epoch(&mut self) -> String {
    self.advance(6)
  }

  pub fn timestamp(&mut self) -> String {
    self.advance(16)
  }

  pub fn uuid(&mut self) -> String {
    self.advance(6)
  }

  pub fn new(s: &String) -> Self {
    Cursor { msg: s.to_owned(), now: 0 }
  }

  pub fn rest(&mut self) -> String {
    let s = String::from(&self.msg[self.now..]);
    self.now = self.msg.len();
    s
  }

  pub fn advance_until(&mut self, s: &std::string::String) -> String {
    CursorLogger.info(
      format!("op=advance_until;s={}", s).as_str()
    );

    let mut c = self.now;
    let len = s.len();

    while true {
      let m = &self.msg[c..c+len].to_string();

      if (m == s) {
        break;
      }

      c += 1;
    }

    let res = self.msg[self.now..c].to_string();
    self.now = c;

    res
  }

  pub fn advance_until_changed(&mut self) -> String {
    let start = self.now;

    let mut s: &str = &self.msg.as_str()[start..start+1];
    let mut _s = s.clone();

    while (self.now < self.msg.len()) {
      _s = &self.msg.as_str()[self.now..self.now+1];

      if (s != _s) { 
        break;
      }

      self.now += 1;
    };

    self.msg[start..self.now].to_string()
  }

}



pub fn vec_to_str_vu16(v: &[u16]) -> String {
  let mut s = String::from("");

  for i in v {
    s += &i.to_string();
  }

  s
}

pub struct WindowCursor<'a, T: crate::window::TBitWindow> {
  pub wdw_ref: &'a T,
  pub now: usize,
}

impl <'a> TCursor<String> for WindowCursor<'a, BitWindow> {

  fn advance(&mut self, size: usize) -> String {
    let n = self.now;
    self.now += size;

    vec_to_str_vu16(
      &self.wdw_ref.data[n..self.now]
    )
  }

  fn read(&self, size: usize) -> String {
    vec_to_str_vu16(
      &self.wdw_ref.data[self.now..self.now + size]
    )
  }
}

pub trait DCursor<V> {
  fn advance(&mut self) -> String;
}

pub struct DimensionWindowCursor<T: TBitWindow> {
  pub wdw_ref: Vec<T>,
  pub now: usize,
  pub repeat: bool,
  pub size: usize,
}

impl DimensionWindowCursor<BitWindow> {

  pub fn iter_2d(&mut self) -> Vec<u16> {
    let mut v = vec![];

    for w in self.wdw_ref.iter() {
      v.push(w.data[self.now])
    }

    v  
  }

  pub fn new(arr: Vec<BitWindow>) -> Self {
    let size = arr.len().try_into().unwrap();
    DimensionWindowCursor { 
      wdw_ref: arr, 
      now: 0,
      repeat: true,
      size: size,
    }
  }
}

impl DCursor<String> for DimensionWindowCursor<BitWindow> {

  fn advance(&mut self) -> String {
    self.now += 1;

    if (self.now >= self.size) {
      self.now = 0;
    }

    vec_to_str_vu16(
      &self.iter_2d()
    )
  }

}

pub struct EventRowCursor { }

impl EventRowCursor {

  pub fn parse(buffer: &std::string::String) -> Event {
    let mut data = HashMap::<String, String>::new();
    let mut c = Cursor::new(buffer);
    let mut ref_signals: Vec<String> = vec![];

    let epoch = c.epoch();
    let uuid = c.uuid();
    let ts = c.timestamp();

    let event_type = c.advance(2);
    let ref_signal_count = c.advance(4).convert_u8();

    let refsignal_length = 44;

    for i in (0.. ref_signal_count) {
      let signal = c.advance(refsignal_length);
      //let signal = NodeSignalKeyRefSer::from(&buffer);
      ref_signals.push(signal);
    };

    Event {
      event_key: epoch.to_owned() + &uuid + &ts,
      epoch: epoch,
      event_type: crate::event::EventType::PE(event_type),
      event_subtype: None, //TODO
      ts: crate::tsgen::Timestamp { s: ts },
      buffer: None, //TODO
      result: None, //TODO,
      payload: None, //TODO
      ref_signals: Some(ref_signals),
    }
  }

}

pub struct BitArraySignalKeyCursor {
  pub key: String,
  pub now: u16,
}

impl BitArraySignalKeyCursor {

  pub fn new(k: &std::string::String) -> Self {
    BitArraySignalKeyCursor {
      key: k.to_owned(),
      now: 0
    }
  }

  pub fn bitarr(&self) -> String {
    // 6: epoch; 6: node uuid; 16: timestamp length;
    self.key.as_str()[6+6+16..].to_string()
  }

  pub fn rbit(&self, idx: usize) -> char {
    // 6: epoch; 6: node uuid; 16: timestamp length;
    let ridx = 6+6+16+idx;
    let v = self.key.chars().nth(ridx).unwrap();
    CursorLogger.info(
      format!("op=bit;idx={:?};ridx={:?};current={};value={};", idx, ridx, self.now, v).as_str()
    );
    v
  }

  pub fn bit(&self, idx: usize) -> bool {
    let bit = self.rbit(idx);
    let mut value: Option<bool> = None;

    if (bit == '0') {
      value = Some(false);
    }

    if (bit == '1') {
      value = Some(true);
    }

    value.expect("invalid bit value!")
  }

}