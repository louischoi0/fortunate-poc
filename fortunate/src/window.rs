// ~ 16: node
// 32 64 128 256 512: BitWindow
// 1024 ~: Sparse Bit Window

// node just get window`s uuid and index
// and value be evaluated lazily at finalized time or event generated time.
use crate::{hashlib::uuid, algorithms::shuffle__fisher_yates};
use std::{marker::PhantomData};

pub struct BitWindow {
  pub uuid: String,

  pub size: u64, 
  pub data: Vec<u16>,

  pub divisor: u64,
}

pub struct SBitWindow {
  pub uuid: String,

  pub size: u64,
  pub data: Vec<u16>,

  pub divisor: u64,
}

pub trait TBitWindow {
  
}

impl TBitWindow for BitWindow {}
impl TBitWindow for SBitWindow {}

pub fn __create_bw(size: u64, divisor: u64) -> BitWindow {
  BitWindow {
    uuid: uuid(16),
    data: Vec::<u16>::with_capacity(size as usize + 100),
    size: size,
    divisor: divisor
  }
}

pub trait WindowInitializable<T: TBitWindow> {
  fn create(&self, size: u64, divisor: u64) -> T;
  fn init(&self, w: &mut T, size: u64, divisor: u64);
  fn shuffle_bw__fisher_yates(w: &mut T);
}


pub struct WindowInitializer<T: TBitWindow> { 
  p: PhantomData<T>,
}

impl WindowInitializer<BitWindow> {
  pub fn new() -> Self {
    WindowInitializer::<BitWindow> {
      p: PhantomData::<BitWindow> {}
    }
  }
    
}


pub fn _create_bw(size: u64, divisor: u64) -> BitWindow {
  BitWindow {
    uuid: uuid(16),
    data: Vec::<u16>::with_capacity(size as usize + 100),
    size: size,
    divisor: divisor
  }
}

impl WindowInitializable<BitWindow> for WindowInitializer<BitWindow> {
  fn create(
    &self,
    size: u64,
    divisor: u64
  ) -> BitWindow {
    assert!(size % divisor == 0);
    let mut w = _create_bw(size, divisor);

    self.init(&mut w, size, divisor);
    w
  }

  fn shuffle_bw__fisher_yates(
    w: &mut BitWindow
  ) {
    let new_data = crate::algorithms::shuffle__fisher_yates::<u16>(&mut w.data);
    w.data = new_data;
  }

  fn init(
    &self, 
    w: &mut BitWindow,
    size: u64,
    divisor: u64,
  ) {

    for i in (0..size) {
      if (i % divisor == 0) {
        w.data.push(1);
      }
      else {
        w.data.push(0);
      }
    }
    WindowInitializer::<BitWindow>::shuffle_bw__fisher_yates(w)
  }

}


pub trait WindowShufflable<T> {
  fn shuffle(&self, w: &mut T) -> T;
}

pub struct WindowShuffler<T: TBitWindow> { 
  p: PhantomData<T>,
}
