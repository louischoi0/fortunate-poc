use std::time::{SystemTime,};
use chrono;

#[derive(std::clone::Clone, Debug)]
pub struct Timestamp {
  pub s: std::string::String,
}

impl Timestamp {
  pub fn from_str(s: &String) -> Self {
    Timestamp { s: s.to_owned() }
  }
}

#[derive(Debug)]
pub struct TsEpochPair {
  pub ts: String,
  pub epoch: String,
}

impl TsEpochPair {
  pub fn new(ts: String, epoch: String) -> Self {
    TsEpochPair { ts: ts, epoch: epoch }
  }
}

pub fn get_ts_c() -> Timestamp {
  let now = chrono::offset::Utc::now();
  let format = "%s%6f";
  Timestamp { s: now.format(format).to_string() } 
}

pub fn get_ts() -> String {
  let now = chrono::offset::Utc::now();
  let format = "%s%6f";
  now.format(format).to_string()
}

pub fn get_time() -> String {
  let date = chrono::Local::now();
  date.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn get_epoch() -> String {
  let its = get_ts().parse::<i64>().unwrap();
  format!("{:X}", its / 300000000)
}

pub fn get_prev_epoch() -> String {
  let its = get_ts().parse::<i64>().unwrap();
  format!("{:X}", (its / 300000000) - 1)
}

pub fn get_prev_epoch_e(epoch: &String) -> String {
  String::from(format!("{:X}", u32::from_str_radix(epoch, 16).unwrap() - 1))
}

pub fn get_ts_pair() -> TsEpochPair {
  let ts = get_ts();
  let epoch = get_epoch();

  TsEpochPair::new(ts, epoch)
}