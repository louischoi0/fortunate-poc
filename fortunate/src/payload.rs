use std::collections::HashMap;
use crate::cursor::{ TCursor, Cursor};

pub struct Payload {
  pub data: Option<HashMap<String, String>>,
  pub buffer: Option<std::string::String>,
}

impl Payload {

  pub fn ser(data: &HashMap<String, String>) -> Payload {
    let mut buffer = std::string::String::from("");

    for (k, v) in data.iter() {
      buffer += k;
      buffer += &std::string::String::from(":");
      buffer += v;
      buffer += &std::string::String::from(";");
    }

    Payload {
      data: Some(data.to_owned()),
      buffer: Some(buffer.to_owned()),
    }
  }


  pub fn deser(buffer: &String) -> Payload {
    let mut cursor = crate::cursor::Cursor::new(buffer);
    let kv_count =  cursor.advance(2).parse::<u8>().unwrap();

    let mut data = HashMap::<String, String>::new();

    for _ in 0..(kv_count-1) {
      let key = cursor.advance_until(&std::string::String::from(":"));
      let v = cursor.advance_until(&std::string::String::from(";"));
      data.insert(key, v);
    }

    Payload {
      data: Some(data),
      buffer: Some(buffer.to_owned()),
    }
  }

}
