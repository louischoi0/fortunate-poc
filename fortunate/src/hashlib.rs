use rand::Rng;
use sha2::{Sha256, Sha512, Digest};
use rand::distributions::{Alphanumeric, DistString};

pub fn sha256(s: &std::string::String) -> std::string::String {
  let mut h = Sha256::new();
  h.update(std::string::String::from(s));
  format!("{:X}",h.finalize())
}

pub fn hash_payload(s: &std::string::String) -> std::string::String {
  sha256(s).as_str()[0..16].to_string()
}

pub fn uuid(len: usize) -> String {
  Alphanumeric.sample_string(&mut rand::thread_rng(), len)
}