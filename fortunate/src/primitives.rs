
#[derive(Debug, Clone)]
pub enum DataType {
  S(std::string::String),
  IDX(usize),
  U8(u8),
  U16(u16),
  U32(u32),
  U64(u64),
}

#[derive(Debug, Clone)]
pub struct Pair <K, V>{
  pub k: K,
  pub v: V
}


pub fn dunwrap_s(d: &DataType) -> std::string::String {
  match d {
    DataType::S(x) => x.to_owned(),
    _ => panic!("")
  }
}

pub trait TString {
  fn shuffle(&self) -> String;
  fn shuffle_n(&self, n: usize) -> String;
}

impl TString for std::string::String {
  fn shuffle(&self) -> String {
    let arr = self.as_bytes().to_vec();

    std::string::String::from_utf8(
      crate::algorithms::shuffle__fisher_yates::<u8>(&arr)
    ).expect("failed to shuffle string")
  }

  fn shuffle_n(&self, n: usize) -> String {
    let mut s = self.to_owned();
    for _ in (0..n) {
      s = s.shuffle()
    }
    s
  }
  
}
