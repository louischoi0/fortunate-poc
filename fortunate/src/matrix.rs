use std::collections::HashMap;

use redis::Commands;

use crate::{tsgen, sessions::RedisImpl, node::FNode, finalizer::FortunateNodeSignalFinalizer};

pub struct Matrix {
  /**
   * managing server session and status, nodes, block 
   */
  pub uuid: String, 

  pub epoch: String,
  pub prev_epoch: String, 

  pub status: String, 

  pub is_finializing: bool,
  pub is_genesis: bool, 

  nodsignal_finalizer: FortunateNodeSignalFinalizer,
  
  pub cimpl: crate::sessions::RedisImpl,
  pub created_at: crate::tsgen::Timestamp,
}

impl Matrix {

  async fn new(&self, uuid: &std::string::String) -> Self {
    let region = std::string::String::from("northeast-1");

    Matrix {
      uuid: uuid.to_owned(),

      epoch: std::string::String::from(""), //TODO
      prev_epoch: std::string::String::from(""), //TODO

      is_genesis: false,

      status: std::string::String::from("INITIATED"),
      is_finializing: false,

      nodsignal_finalizer: FortunateNodeSignalFinalizer::new(
        &region
      ).await,

      cimpl: crate::sessions::RedisImpl::new(Some(uuid.to_owned())),
      created_at: tsgen::get_ts_c(),
    }
  }

  fn check_exit_flag_and_terminate(&self, session: &HashMap<String, String>) -> bool {
    false
  }

  fn process(&mut self) {
    let interval = 10;
    let mut _epoch: Option<&String> = None;
    let mut _prev_epoch: Option<&String> = None;

    loop {
      let session = self.get_matrix_session();
      _epoch = session.get(
        &std::string::String::from("epoch")
      );

      match _epoch {
        Some(e) => {
          if (self.epoch != _epoch.unwrap().to_owned()) {

          }
          else {

          }
        }
        None => {}
      }

      let is_genesis = 
        session.get(
          &std::string::String::from("is_genesis")
        ).unwrap();
      
      let exit = self.check_exit_flag_and_terminate(&session);

      if (is_genesis == "true") {

      }
      
      else {

      }


      if (exit) {
        break;
      }
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
      self.is_genesis.to_string(),
    );

    data.insert(
      std::string::String::from("is_genesis"),
      self.epoch.to_owned()
    );

    data
  }

  async fn init_matrix_genesis(
    &self
  ) -> () {

  }

  async fn spawn_node(&mut self) {
    let uuid = crate::hashlib::uuid(16);
    let _uuid = uuid.to_owned();
     
    let handle  = 
      tokio::spawn(async move {
        let mut node0 = crate::node::FNode::new(uuid.to_owned()).await;
        node0.process().await;
      });
    
  }

  pub fn terminate_node_callback(
    node: &mut FNode
  ) {
    let key = node.region.to_owned() + ":nodelist";

    node.session.cimpl.redis_connection.lrem::<String,String,String> (
      key, 0, std::string::String::from (
        node.uuid.to_owned()
      )
    );

    println!("remove node: '{}'", node.uuid.to_owned())

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


struct ObjectLock {
  object_uuid: String, 
  holder: u64,

  lock_type: u8,
  
  last_status: u8,  
  last_updated_at: u64,
}

impl ObjectLock { }

pub struct ObjectSession {
  pub uuid: String,
  pub object_type: String,

  // object_lock: ObjectLock, TODO


  /**
   * 0 no flag 
   * 1 
   * 2 
   * 4 terminate flag
   */
  pub flag: String,
  pub status: String,

  pub updated_at: u64,
  pub created_at: u64,

  pub cimpl: crate::sessions::RedisImpl,
}

impl ObjectSession {
  pub fn new(uuid: String, object_type: String) -> Self {
    ObjectSession { 
      uuid: uuid.to_owned(), 
      object_type: object_type, 
      cimpl: crate::sessions::RedisImpl::new(Some(uuid)), 
      flag: std::string::String::from(""), 
      status: std::string::String::from(""),
      updated_at: 0, 
      created_at: tsgen::get_ts().parse::<u64>().unwrap() 
    }
  }

  pub fn set_status(&mut self, s: &String) {
    self.cimpl.set::<String, String> (
      std::string::String::from("status"),
      s.to_owned()
    );
    self.timestamp()
  }

  pub fn get_flag(&mut self) -> std::string::String {
    self.cimpl.get::<String>(
      std::string::String::from("flag")
    )
  }

  pub fn timestamp(&mut self) {
    return;
    self.cimpl.set::<u64, u64> (
      std::string::String::from("created_at"), 
      tsgen::get_ts().parse::<u64>().unwrap()
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

  pub fn read_flag(&self) {
  }
}
