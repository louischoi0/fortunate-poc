use rand::Rng;

pub fn get_randnum_r(min: u32, max: u32) -> u32 {
  let mut rng = rand::thread_rng();
  (rng.gen::<u32>() & max) + min
}



