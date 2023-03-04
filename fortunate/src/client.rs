use std::{collections::HashMap, hash::Hash};

use clap::{arg, Command};
use redis::Commands;
use tokio::time::{sleep, Duration};

use crate::finalizer::{FortunateEventFinalizer, BlockVerifiable, BlockFinalizable};
use crate::flog::FortunateLogger;
use crate::primitives::dunwrap_s;
use crate::{payload, hashlib, dynamoc};
use crate::fnode::{INodeImpl_S01, INode};
use crate::sessions::RedisImpl;
use crate::{finalizer::FortunateNodeSignalFinalizer};
use crate::eventgenerator::EventGenerator;
use crate::actionplanner::{ActionPlanner, ExpMap, IActionPlan, BitArrayActionPlan};
use crate::benchmark::{benchmark_main, benchmark_node};

const ClientLogger: FortunateLogger = FortunateLogger::new("client");

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
        .subcommand(
                Command::new("stats")
                .about("generate stats for a plan")
        )
}

pub async fn generate_event_periodically(pe: &mut EventGenerator) -> () {
  ClientLogger.info(
    format!("op:generate_event_periodically;").as_str()
  );
  let loop_interval = 5000;
  let commiter = crate::event::EventCmtr::new().await;

  let mut ap = ActionPlanner::new().await;
  let em = ExpMap { e2: 4, e10: 3 };

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
    let dynamo_client = dynamoc::get_dynamo_client().await;

    let plan = ap.get_actionplan_for_event_pe::<bool>(&dynamo_client, &epoch, &em).await;

    let res = pe.generate_event_from_plan(plan, &"".to_string()).await; //TODO
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
        //let session = FNode::get_node_session(&mut imp, it);
        //println!("{:?}", session);
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

        let region = std::string::String::from("matrix:northeast-1"); 
        let mut nd = INodeImpl_S01::new(&region).await;
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
          &std::string::String::from("northeast-1")
        ).await;

        matrix.process().await;
       },

      Some(("pubevent", sub_matches)) => {
        let region = "northeast-1".to_string();
        let mut pe = EventGenerator::new(&region);

        generate_event_periodically(&mut pe).await;

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
      },

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
      Some(("stats", _sub_matches)) => {
        //benchmark_main().await;
        benchmark_node().await;
      }

       _ => panic!("panic here."),
   }

}

