use std::{collections::HashMap, hash::Hash};

use clap::{arg, Command};
use redis::Commands;
use tokio::time::{sleep, Duration};

use crate::finalizer::{FortunateEventFinalizer, BlockVerifiable, BlockFinalizable};
use crate::primitives::dunwrap_s;
use crate::{payload, hashlib};
use crate::{node::FNode, sessions::RedisImpl};
use crate::{finalizer::FortunateNodeSignalFinalizer};


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
        .subcommand(
          Command::new("start")
                .about("start fortunate")
        )
        .subcommand(
          Command::new("finalize")
                .arg(arg!(<COMPONENT> "component"))
                .arg(arg!(<EPOCH> "epoch"))
                .arg(arg!(<PREVEPOCH> "prevepoch"))
                .about("finalize event block")
        )
        .subcommand(
          Command::new("verify")
                .about("verify block or signals or event")
                .arg(arg!(<COMPONENT> "component"))
                .arg_required_else_help(true)
                .arg(arg!(<EPOCH> "epoch"))
                .arg_required_else_help(true)
        )
        .subcommand(
          Command::new("dgenerate")
                .about("generate some events and it is for development")
        )
      
}

pub async fn generate_event_periodically(pe: &mut crate::event::PEventGenerator) -> () {
  let loop_interval = 5000;
  let commiter = crate::event::EventCmtr::new().await;

  while (true) {
    let mut payload = HashMap::<String,String>::new();
    payload.insert(
      std::string::String::from("nonce"), 
      hashlib::uuid(8)
    );

    payload.insert(
      std::string::String::from("token"), 
      std::string::String::from("abcdefghijklmnopqrstuvwxyz"),
    );

    let payload = payload::Payload::ser(&payload);

    let epoch = 
      crate::matrix::Matrix::get_prev_epoch(&pe.region, &mut pe.cimpl, &pe.uuid).await;
    
    let p = payload.buffer.unwrap().to_owned();

    let res = pe.generate_event_pe2(&p).await.unwrap();
    println!("peventgenerator: {}:{}, {:?}", epoch, p, res);

    commiter.commit_event(&res).await;
    sleep(Duration::from_millis(loop_interval)).await;
  }


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
        let nodeid = format!("node:{}", nodeid);

        let region = std::string::String::from("matrix:northeast-1"); 
        let mut nd = FNode::new(&nodeid, &region).await;
        nd.process().await;
       },

      Some(("terminate", sub_matches)) => {
        println!( "terminate node: {}", sub_matches.get_one::<String>("NODEID").expect("required"));
        let nodeid = sub_matches.get_one::<String>("NODEID").unwrap();
        let nodeid = format!("node:{}", nodeid);

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

      Some(("start", sub_matches)) => {
        println!("start fortunate matrix");
        let mut matrix = crate::matrix::Matrix::new(
          &std::string::String::from("matrix:northeast-1")
        ).await;

        matrix.process().await;
       },

      Some(("pubevent", sub_matches)) => {

        let mut pev = crate::event::PEventGenerator::new(
          &std::string::String::from("matrix:northeast-1")
        ).await;

        generate_event_periodically(&mut pev).await;

      },

      Some(("finalize", sub_matches)) => {
        let component = sub_matches.get_one::<String>("COMPONENT").expect("required.");

        if (component == "event") {
          let epoch = sub_matches.get_one::<String>("EPOCH").expect("required.");
          let prev_epoch = sub_matches.get_one::<String>("PREVEPOCH").expect("required.");
          
          let fnz = crate::finalizer::FortunateEventFinalizer::new(
            &std::string::String::from("northeast-1")
          ).await;
          
          fnz.finalize_eventblock(
            epoch, 
            Some(prev_epoch)
          ).await;
        };
      }

      Some(("verify", sub_matches)) => {
        let component = sub_matches.get_one::<String>("COMPONENT").expect("required.");
        let epoch = sub_matches.get_one::<String>("EPOCH").expect("required.");

        println!("verify {}: {}", component, epoch);

        let region = std::string::String::from("northeast-1");

        if (component == "signalblock") {
          let fnz  = 
              FortunateNodeSignalFinalizer::new(&region).await;

          fnz.logger.info(
            format!("verify nodesignalblock {}", epoch).as_str()
          );

          let verified = fnz.verify_nodesignalblock(epoch).await;

          assert!(verified);

          fnz.logger.info(
            format!("successfully verified: nodesignalblock {}", epoch).as_str()
          );
        }

        else if (component == "eventblock") {

          let fnz = 
            FortunateEventFinalizer::new(&region).await;

          let s: &dyn BlockFinalizable<crate::event::Event> = &fnz;
          let v: &dyn BlockVerifiable<crate::event::Event> = &fnz;


          let hm_block = s.get_block(epoch).await;
          v._get(
            &dunwrap_s(
              hm_block.get("hash").unwrap())
          );

          let block_hash = dunwrap_s(
            hm_block.get("hash") .unwrap()
          );

          let events = fnz.get_events(epoch).await.unwrap();

          for e in events.iter() {
            println!("{:?}", e.get("buffer").unwrap())
          };

          println!("{:?}", events.len());

          let verified = v.verify_block(&hm_block, &events);

          if (verified) {
            println!("Verified Successfully.")
          }
          else {
            println!("Verify Failed.")
          }
        }

       },

       _ => panic!("panic here."),
   }

}

