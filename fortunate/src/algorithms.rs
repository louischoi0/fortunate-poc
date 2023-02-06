use rand::Rng;

pub fn get_randnum_r(min: u32, max: u32) -> u32 {
  let mut rng = rand::thread_rng();
  (rng.gen::<u32>() & max) + min
}



pub fn shuffle__fisher_yates<T: Clone + Copy>(arr: &Vec<T>) -> Vec<T> {
  let mut result: Vec<T> = vec![];
  let mut _arr = arr.clone();

  while( _arr.len() > 0 ) {
    let idx = usize::try_from(
      get_randnum_r(0, u32::try_from(_arr.len()-1).unwrap())
    ).unwrap();
    let last_idx = _arr.len() - 1;

    let temp = _arr.get(last_idx).unwrap().to_owned();

    _arr[last_idx] = _arr[idx];
    _arr[idx] = temp.to_owned();

    result.push(
      *_arr.get(idx).unwrap()
    );

    _arr.remove(idx);
  }
  return result;
}
