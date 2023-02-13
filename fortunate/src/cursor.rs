use crate::{window::{BitWindow, TBitWindow}, fnode::BitArraySignalKey};

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

  pub fn new(s: &String) -> Self {
    Cursor { msg: s.to_owned(), now: 0 }
  }

  pub fn rest(&mut self) -> String {
    let s = String::from(&self.msg[self.now..]);
    self.now = self.msg.len();
    s
  }

  pub fn advance_until(&mut self, s: &std::string::String) -> String {
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
    DimensionWindowCursor { 
      wdw_ref: arr, 
      now: 0 
    }
  }
}

impl DCursor<String> for DimensionWindowCursor<BitWindow> {

  fn advance(&mut self) -> String {
    self.now += 1;

    vec_to_str_vu16(
      &self.iter_2d()
    )
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
    // 6: epoch length; 16: timestamp length;
    self.key.as_str()[6+16..].to_string()
  }

  pub fn bit(&self, idx: usize) -> bool {
    let idx = 6+16+idx;
    let bit = self.key.chars().nth(idx).unwrap();

    if (bit == '0') {
      false
    }
    else {
      true
    }
  }

}