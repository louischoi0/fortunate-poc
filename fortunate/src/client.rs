use std::collections::HashMap;

use clap::{arg, Command};

use crate::{node::FNode, sessions::RedisImpl};
use redis::Commands;

pub fn cli() -> Command {
    Command::new("/usr/local/bin/fortunate")
        .about("fortunate api cli")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("runnode")
                .about("run node")
                .arg(arg!(<NODEID> "nodeid"))
                .arg_required_else_help(true),
        )
        .subcommand(
            Command::new("terminate")
                .about("terminate node")
                .arg(arg!(<NODEID> "nodeid"))
                .arg_required_else_help(true),
        )
        .subcommand(
          Command::new("nodelist")
                .arg(arg!(<REGIONID> "region"))
                .about("list nodes active")
        )
}
pub fn query_client_status(region: &std::string::String) {
  let mut imp = RedisImpl::new(None);

  let nodes = imp.redis_connection.lrange::<String, Vec<String>>(
    region.to_owned() + ":nodelist",
    0,
    100
  );


  match nodes {
    Ok(x) => {
      for it in x.iter() {
        let session = FNode::get_node_session(&mut imp, it);
        println!("{:?}", session);
      }
    }
    Err(e) => {
      println!("{:?}", e);
    }
  };

}

pub async fn client_main() {
   let matches = cli().get_matches();

   match matches.subcommand() {
       Some(("runnode", sub_matches)) => {
         println!( "run node: {}", sub_matches.get_one::<String>("NODEID").expect("required"));
         let nodeid = sub_matches.get_one::<String>("NODEID").unwrap();
         let mut nd = FNode::new(nodeid.to_owned()).await;
         nd.process().await;
       },

       Some(("terminate", sub_matches)) => {
        println!( "terminate node: {}", sub_matches.get_one::<String>("NODEID").expect("required"));
        let nodeid = sub_matches.get_one::<String>("NODEID").unwrap();

        let mut imp = RedisImpl::new(Some(nodeid.to_owned()));
        imp.set::<String, String>( 
          std::string::String::from("flag"), 
          std::string::String::from("TERMINATED"),
        );

       },

       Some(("nodelist", sub_matches)) => {
        println!("query nodelist.");
        let region = sub_matches.get_one::<String>("REGIONID").unwrap();
        query_client_status(region);
       },

       _ => panic!("panic here.")
   }

}

