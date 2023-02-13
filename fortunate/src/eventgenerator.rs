use crate::event::{EventBuffer, EventType, Event, EventResult};
use crate::{matrix, sessions::RedisImpl, actionplanner::IActionPlan};
use crate::{tsgen, payload};
use crate::hashlib;


pub struct EventGenerator {

  pub uuid: std::string::String,
  pub region: std::string::String,
  pub cimpl: RedisImpl,
}

impl EventGenerator {

  pub fn new(region: &std::string::String) -> Self {
    let _uuid = hashlib::uuid(6);
    EventGenerator { uuid: _uuid.to_owned(), region: region.to_owned(), cimpl: crate::sessions::RedisImpl::new(Some(_uuid.to_owned())) }
  }

  fn build_event_buffer(
    &self,
    event_type: &EventType,
    epoch: &std::string::String, 
    payload: &std::string::String,
    ts: &tsgen::Timestamp,
    ref_signals: &Vec<crate::node::NodeSignalKeyRefSer>,
  ) -> EventBuffer {

    let mut buffer = epoch.to_owned() + &self.uuid + &ts.s;
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
    };

    EventBuffer { event_key: event_key, buffer: buffer }
  }


  pub async fn generate_event_from_plan<T: IActionPlan<bool>>(
    &mut self, 
    plan: T,
    s_payload: &std::string::String,
  ) -> Event {
    let ts = tsgen::get_ts_c();

    let epoch = 
      matrix::Matrix::get_prev_epoch(
        &self.region, 
        &mut self.cimpl, 
        &self.uuid
      ).await;


    let buffer = self.build_event_buffer(
      &plan.event_type(), 
      &epoch, 
      s_payload, 
      &ts, 
      &plan.signals()
    );
    
    Event::new(
          &epoch, 
          &buffer.event_key, 
          &plan.event_type(), 
          &buffer, 
          Some(EventResult::TF(plan.act())), 
          Some(s_payload.to_owned())
        )

  }
}
