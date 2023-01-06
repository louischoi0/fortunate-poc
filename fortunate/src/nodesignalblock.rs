pub struct NodeSignal {
  pub epoch: String,
  pub signal_key: String,
  pub group: String,
  pub timestamp: String, 
  pub signal_value: String, 
  pub data: Option<String>,
}



pub struct NodeSignalBlock {
  pub epoch: String,
  pub hash: String, 

  pub timestamp: u32,
  pub finalized_at: u32,
  pub prev_block_hash: String,

  signals: Option<Vec<NodeSignal>>,
  verified: bool, 
  finalized: bool,
}






