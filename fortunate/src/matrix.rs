use std::collections::HashMap;
use std::future::Future;
use tokio::time::{sleep, Duration};
use redis::{Commands, RedisError};

use crate::finalizer::{BlockFinalizable, FortunateEventFinalizer};
use crate::{tsgen, sessions::RedisImpl, node::FNode, finalizer::FortunateNodeSignalFinalizer};
use crate::flog::FortunateLogger;
use async_trait::async_trait;

const MatrixLogger: crate::flog::FortunateLogger  = crate::flog::FortunateLogger::new("matrix");


pub struct Matrix {
  /**
   * managing server session and status, nodes, block 
   */
  pub uuid: String, 

  pub epoch: String,
  pub prev_epoch: Option<String>, 

  pub status: String, 

  pub is_finializing: bool,
  pub is_genesis: bool, 

  nodsignal_finalizer: FortunateNodeSignalFinalizer,
  event_finalizer: FortunateEventFinalizer,

  logger: &'static FortunateLogger,
  
  pub cimpl: crate::sessions::RedisImpl,
  pub created_at: crate::tsgen::Timestamp,
}

#[async_trait]
pub trait MatrixComponent {

  /* component can only hold shared lock. */
  async fn matrix_lock_acquire(
    &mut self
  ) -> bool;

}


impl Matrix {

  pub async fn get_prev_epoch(
    region: &std::string::String,
    cimpl: &mut RedisImpl,
    requester: &std::string::String,
  ) -> std::string::String {

    //MatrixLogger.info( format!("op:get_prev_epoch; matrix:{};", region).as_str());
    //ObjectLock::acquire(cimpl, "matrix", region, requester, 0).await;

    cimpl.redis_connection.get::<String,String>( format!("matrix:{}:prev_epoch", region)).unwrap()
  }

  pub async fn get_epoch(
    region: &std::string::String,
    cimpl: &mut RedisImpl,
    requester: &std::string::String,
  ) -> std::string::String {

    MatrixLogger.info(
      format!("op:get_epoch; matrix:{};", region).as_str()
    );

    ObjectLock::acquire(
      cimpl, 
      "matrix",
      region, requester, 0
    ).await;

    let res = cimpl.redis_connection.get::<String,String>(
      format!("matrix:{}:epoch", region)
    );

    match res {
      Ok(x) => x,
      e=> { 
        panic!("{:?}",e)
      }
    }

  }

  pub async fn new(uuid: &std::string::String) -> Self {
    
    let mut matrix = Matrix {
      uuid: uuid.to_owned(),

      epoch: tsgen::get_epoch(), //TODO from redis session
      prev_epoch: None, //TODO from redis session

      is_genesis: false,

      status: std::string::String::from("INITIATED"),
      is_finializing: false,

      nodsignal_finalizer: FortunateNodeSignalFinalizer::new(
        &uuid[7..uuid.len()].to_string() //TODO CHECK
      ).await,

      event_finalizer: FortunateEventFinalizer::new(
        uuid
      ).await,

      logger: &MatrixLogger,
      cimpl: crate::sessions::RedisImpl::new(Some(format!("matrix:{}",uuid.to_owned()))),
      created_at: tsgen::get_ts_c(),
    };

    ObjectLock::init_object_lock(&mut matrix.cimpl, "matrix", uuid);

    matrix
  }

  fn check_exit_flag_and_terminate(&self, session: &HashMap<String, String>) -> bool {
    false
  }

  async fn start_finalizing(&mut self) {
    ObjectLock::acquire(
      &mut self.cimpl, 
      "matrix",
      &self.uuid, 
      &self.uuid, 
      1
    );
  }

  async fn end_finalizing(&mut self) {
    ObjectLock::release(
      &mut self.cimpl, "matrix", &self.uuid
    );
  }

  async fn commit_tblock<T> (
    &self,
    fnz: &dyn BlockFinalizable<T>
  ) {
  // -> crate::block::BlockHash {

  }

  async fn commit_component_blocks(
    &self,
    _epoch: &std::string::String,
    _prev_epoch: &Option<std::string::String>,
  ) {

    let nodesignalblock_hash = self.nodsignal_finalizer.finalize_nodesignalblock(
        &String::from(_epoch), 
        _prev_epoch.as_ref()
    )
        .await
        .expect("nodesignal block commit failed");

    let eventblock_hash = self.event_finalizer.finalize_eventblock(
        _epoch, _prev_epoch.as_ref()
     ).await
      .expect("nodesignal block commit failed");
  }

  pub async fn epoch_changed_callback(
    &mut self, 
    _epoch: std::string::String
  ) {
    self.start_finalizing().await;

    self.commit_component_blocks(
      &self.epoch, &self.prev_epoch
    ).await;

    self.prev_epoch = Some(self.epoch.to_owned());
    self.epoch = _epoch;

    self.commit_matrix_session();
    self.end_finalizing().await;
  }

  pub async fn process(&mut self) -> () {
    let interval = 10;

    let mut _epoch: String = self.epoch.to_owned();
    let mut session = HashMap::<String, String>::new();
    let mut loop_cnt: u64 = 0;

    loop {
      loop_cnt += 1;

      if (loop_cnt % 1200 == 0) {
        self.logger.info("session is working.");
      }

      _epoch = tsgen::get_epoch();

      if (_epoch != self.epoch) {
        self.epoch_changed_callback(_epoch).await;
      }

      if (self.check_exit_flag_and_terminate(&session)) { break; }

      sleep(Duration::from_millis(interval)).await;
    }

  }

  fn commit_matrix_session(&mut self) -> () {
    let data = self.get_matrix_session();

    for (k, v) in data {
      self.cimpl.set::<String, String>(k.to_owned(), v.to_owned());
    }

  }

  fn get_matrix_session(&self) -> HashMap<String, String> {
    let mut data = HashMap::<String, String>::new();

    data.insert(
      std::string::String::from("epoch"),
      self.epoch.to_owned()
    );

    data.insert(
      std::string::String::from("prev_epoch"),
      self.prev_epoch.to_owned().unwrap(),
    );

    data
  }

  pub fn terminate_node_callback(
    node: &mut FNode
  ) {
    let key = node.region.to_owned() + ":nodelist";

    let result = node.session.cimpl.redis_connection.lrem::<String,String,String> (
      key, 0, std::string::String::from (
        node.uuid.to_owned()
      )
    );

    match &result {
      Ok(x) => {},
      Err(e) => {
        panic!("err!")
      }
    }
  }

  pub fn new_node_callback(
    node: &mut FNode
  ) {
    let key = node.region.to_owned() + ":nodelist";
    node.session.cimpl.redis_connection.lpush::<String,String,String>(
      key, 
      node.uuid.to_owned()
    );
  }
  

}

const ObjectLockLogger: crate::flog::FortunateLogger  = crate::flog::FortunateLogger::new("objectlock");

pub struct ObjectLock { }


impl ObjectLock { 

  fn parse_key(
    object_type: &str,
    object_uuid: &std::string::String,
    lock_type: &'static str,
  ) -> std::string::String {
    format!("{}:{}:{}", object_type, object_uuid, lock_type)
  }

  fn get(
    cimpl: &mut RedisImpl,
    object_type: &str,
    object_uuid: &std::string::String,
    lock_type: &'static str,
  ) -> std::string::String {

    let key = ObjectLock::parse_key(object_type, object_uuid, lock_type);

    ObjectLockLogger.info(
      format!("getop; key:{:?};", key.to_owned())
            .as_str()
    );

    cimpl.redis_connection.get::<String, String>(
      key,
    ).expect("")
  }

  fn set(
    cimpl: &mut RedisImpl,
    object_type: &str,
    object_uuid: &std::string::String,
    lock_type: &'static str,
    v: &std::string::String,
  ) -> () {
    let key = ObjectLock::parse_key(object_type, object_uuid, lock_type);
    cimpl.redis_connection.set::<String, String, String> (
      key, v.to_owned()
    );
  }

  pub fn init_object_lock(
    cimpl: &mut RedisImpl,
    object_type: &str,
    object_uuid: &std::string::String,
  ) -> () {
    ObjectLock::set(
      cimpl, object_type, object_uuid, "object_lock", 
      &std::string::String::from("0")
    );

    ObjectLock::set(
      cimpl, object_type, object_uuid, "created_at", 
      &tsgen::get_ts(),
    );
  }

  pub async fn release(
    cimpl: &mut RedisImpl,
    object_type: &str,
    object_uuid: &std::string::String,
  ) -> () {
    ObjectLock::set(
      cimpl, object_type, object_uuid, "object_lock", 
      &std::string::String::from("0")
    );

    ObjectLock::set(
      cimpl, object_type, object_uuid, "last_released_at", 
      &tsgen::get_ts()
    );

  }

  pub async fn acquire(
    cimpl: &mut RedisImpl,
    object_type: &str,
    object_uuid: &std::string::String,
    requester: &std::string::String,
    mode: u8
  ) -> bool {
    let interval = 10;
    ObjectLockLogger.info(
      format!("Acquire for {} from {}; object_type:{:?}; mod:{:?};", object_type, object_uuid, requester, mode)
            .as_str()
    );

    loop {
      let lock = ObjectLock::get(cimpl, object_type, object_uuid, "object_lock");

      if (lock == "0") {
        ObjectLock::set(cimpl, object_type, object_uuid, "holder", requester);
        ObjectLock::set(cimpl, object_type, object_uuid, "last_acquired_at", &tsgen::get_ts());

        /**
         * 0 shared lock
         * 1 exclusive lock
         */
        if (mode == 1) {
          ObjectLock::set(
            cimpl, object_type, object_uuid, "object_lock", &std::string::String::from("1"), 
          );
        }

        break;
      }
      else {
        sleep(Duration::from_millis(interval)).await;
      }

    }

    true
  }

}

pub struct ObjectSession {
  pub uuid: String,
  pub object_type: String,

  // object_lock: ObjectLock, TODO

  pub updated_at: u64,
  pub created_at: u64,

  pub cimpl: crate::sessions::RedisImpl,
}

impl ObjectSession {
  pub fn new(uuid: String, object_type: String) -> Self {

    ObjectLockLogger.info(
      format!("op:init_session; uuid:{};", uuid).as_str()
    );

    let mut inst = ObjectSession { 
      uuid: uuid.to_owned(), 
      object_type: object_type.to_owned(), 
      cimpl: crate::sessions::RedisImpl::new(Some(
        format!("{}:{}", object_type, uuid)
      )), 
      updated_at: 0, 
      created_at: tsgen::get_ts().parse::<u64>().unwrap() 
    };

    inst.cimpl.set::<String, String>(std::string::String::from("crated_at"), tsgen::get_ts());
    inst
  }

  pub fn set_status(&mut self, s: &String) {
    self.cimpl.set::<String, String> (
      std::string::String::from("status"),
      s.to_owned()
    );
    self.timestamp()
  }

  pub fn timestamp(&mut self) {
    self.cimpl.set::<String, String> (
      std::string::String::from("updated_at"), 
      tsgen::get_ts()
    )
  }

  pub fn initialize(&mut self) {
    self.cimpl.set::<String, String>(
      std::string::String::from("flag"), 
      std::string::String::from("0"), 
    );

    self.cimpl.set::<String, String>(
      std::string::String::from("object_type"), 
      self.object_type.to_owned()
    );

    self.cimpl.set::<String, String>(
      std::string::String::from("status"), 
      self.object_type.to_owned()
    );

  }

}
