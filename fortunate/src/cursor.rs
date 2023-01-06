
#[derive(Debug)]
pub struct Cursor {
  pub msg: String,
  pub now: usize,
}

impl Cursor {
  pub fn advance(&mut self, size: usize) -> String {
    let s = String::from(&self.msg[self.now..self.now+size]);
    self.now += size;
    s
  }

  pub fn new(s: String) -> Self {
    Cursor { msg: s, now: 0 }
  }

  pub fn read(&self, size: usize) -> String {
    let s = String::from(&self.msg[self.now..self.now+size]);
    s 
  }

  pub fn rest(&mut self) -> String {
    let s = String::from(&self.msg[self.now..]);
    self.now = self.msg.len();
    s
  }
}