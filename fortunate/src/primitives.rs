
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