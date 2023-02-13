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


struct TreeNode<'a, T> {
  value: T,

  left: Option<&'a TreeNode<'a, T>>,
  right: Option<&'a TreeNode<'a, T>>,
}

struct Tree<'a, T> {
  root: Option<TreeNode<'a, T>>,
}

impl <'a, T>TreeNode<'a, T> {
  pub fn new(v: T) -> Self {
    TreeNode { value: v, left: None, right: None } 
  }

  pub fn left(&mut self, v: &'a TreeNode<'a, T>) {
    self.left = Some(v);
  }

  pub fn right(&mut self, v: &'a TreeNode<'a, T>) {
    self.left = Some(v);
  }

}

impl <'a, T>Tree<'a, T> {

  fn insert_node_l(&self, n: &'a mut TreeNode<'a, T>, v: &'a TreeNode<'a, T>)  {
    n.left(&v);
  }

  fn insert_node_r(&self, n: &'a mut TreeNode<'a, T>, v: &'a TreeNode<'a, T>)  {
    n.right(&v);
  }

}

struct TreeCursor<'a, T> {
  root: &'a TreeNode<'a, T>,
  now: Option<&'a TreeNode<'a, T>>,
}

impl <'a, T> TreeCursor<'a, T> {

  fn get_right_or(&self, t: TreeNode<'a, T>) -> Option<&'a TreeNode<'a, T>> {
    match t.right {
      Some(v) => {
        Some(v)
      }, 
      None => None
    }
  }

  fn get_left_or(&self, t: TreeNode<'a, T>) -> Option<&'a TreeNode<'a, T>> {
    todo!()
  }

  fn next(&mut self) -> Option<&'a TreeNode<'a, T>> {
    match self.now {
      Some(x) => {
        None
      },
      None => {
        None
      }
    }
  }

}


