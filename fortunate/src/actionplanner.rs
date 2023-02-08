use rand::{Rng, thread_rng};

use crate::primitives::{Pair, dunwrap_s};
use crate::{algorithms, primitives::TString};
use crate::fnode::get_node_s_signals;
use crate::dynamoc::{get_dynamo_client};
use aws_sdk_dynamodb::{ Client, };

pub struct ActionPlanner {
  status: std::string::String,
  dynamo_client: Client
}

pub struct ExpMap {
  pub e2: u64, // a
  pub e10: u64, // b
}

impl ExpMap {
  pub fn new(e2: u64, e10: u64) -> Self {
    ExpMap { e2: e2, e10: e10 } 
  }
}

pub trait IActionPlan<T> {
  fn reduce(&self) -> T;
}

struct BitArrayActionPlan<T> {
  pub signal_key_ref_pairs: Vec<Pair<String, u8>>,
  pub result: T,
}

impl IActionPlan<bool> for BitArrayActionPlan<bool> {

  fn reduce(&self) -> bool {
    true
  }

}

impl ActionPlanner {

  pub async fn new() -> Self {
    ActionPlanner { status: "".to_string(), dynamo_client: get_dynamo_client().await }
  }

  pub async fn get_actionplan_for_event(&self, epoch: &std::string::String, expmap: &ExpMap) -> Vec<Pair<String, u8>> {
    let signals 
        = get_node_s_signals(&self.dynamo_client, epoch).await;

    let estr = self.range_expmap(expmap);

    let mut c = crate::cursor::Cursor::new(&estr);

    let mut signal_idx = 0;
    let mut key_ref_pairs: Vec<Pair<String, u8>> = vec![];

    loop {
      let s = c.advance_until_changed();
      let exp = s.len();
      let signal = signals.get(signal_idx).unwrap();
      
      if ( s.chars().nth(0).unwrap() == 'a') {
        let p = Pair::<String, u8> { 
          k: dunwrap_s(
            signal.get("signal_key").unwrap()
          ),
          v: (exp-1).try_into().unwrap(), 
        };
        key_ref_pairs.push(p);
      }

      else if (s.chars().nth(0).unwrap() == 'b') {
        let p = Pair::<String, u8> { 
          k: dunwrap_s(
            signal.get("signal_key").unwrap()
          ),
          v: (exp-1+10).try_into().unwrap(), 
        };
        key_ref_pairs.push(p);
      }

      signal_idx = (signal_idx + 1) % signals.len();

      if (c.eof()) {
        break;
      }
    }
    
    key_ref_pairs
  }

  fn range_expmap(&self, expmap: &ExpMap) -> String {
    let a = "a".repeat(expmap.e2.try_into().unwrap()) + &"b".repeat(expmap.e10.try_into().unwrap());
    a.shuffle_n(3)
  }

}


