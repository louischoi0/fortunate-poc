use log::{debug, error, info, trace, warn};
const LOGGING_FILE: &str = "log4rs.yaml";

#[derive(Debug)]
pub struct FortunateLogger {
  pub program_name: String,
}

impl FortunateLogger {
  pub fn init() {
    log4rs::init_file(LOGGING_FILE, Default::default()).unwrap();
  }

  pub fn new(program_name: String) -> Self {
    let logger = FortunateLogger {
      program_name: program_name
    };
    logger
  }
 
  pub fn debug(&self, msg: &str) {
    debug!("{}", msg);
  }

  pub fn error(&self, msg: &str) {
    error!("{}", msg);
  }

  pub fn info(&self, msg: &str) {
    info!("{}", msg);
  }

  pub fn trace(&self, msg: &str) {
    trace!("{}", msg);
  }

  pub fn warn(&self, msg: &str) {
    warn!("{}", msg);
  }

}