
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

  pub fn new(s: &String) -> Self {
    Cursor { msg: s.to_owned(), now: 0 }
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

}