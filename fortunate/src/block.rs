pub struct BlockBuffer {
  pub buffer: std::string::String,
}

impl PartialEq for BlockBuffer {
  fn eq(&self, rhs: &Self) -> bool {
    self.buffer == rhs.buffer
  }
}

#[derive(std::clone::Clone)]
pub struct BlockHash {
  pub hash: std::string::String
}

impl PartialEq for BlockHash {
  fn eq(&self, rhs: &Self) -> bool {
    self.hash == rhs.hash
  }
}
