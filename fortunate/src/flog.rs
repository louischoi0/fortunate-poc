use log::{debug, error, info, trace, warn};
const LOGGING_FILE: &str = "log4rs.yaml";

#[derive(Debug, Clone)]
pub struct FortunateLogger {
  pub program_name: &'static str,
}

impl FortunateLogger {
  pub fn init() {
    log4rs::init_file(LOGGING_FILE, Default::default()).unwrap();
  }

  pub const fn new(program_name: &'static str) -> Self {
    let logger = FortunateLogger {
      program_name: program_name
    };
    logger
  }
 
  pub fn debug(&self, msg: &str) {
    debug!("{}:{}", self.program_name, msg);
  }

  pub fn error(&self, msg: &str) {
    error!("{}:{}", self.program_name, msg);
  }

  pub fn info(&self, msg: &str) {
    info!("{}:{}", self.program_name, msg);
  }

  pub fn trace(&self, msg: &str) {
    trace!("{}:{}", self.program_name, msg);
  }

  pub fn warn(&self, msg: &str) {
    warn!("{}:{}", self.program_name, msg);
  }

}