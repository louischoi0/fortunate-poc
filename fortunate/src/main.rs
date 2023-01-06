use rand::Rng;

extern crate log4rs;

mod node;
mod dynamoc;
mod tsgen;
mod flog;
mod cursor;
mod sessions;
mod finalizer;
mod block;
mod event;
mod algorithms;

use log::{debug, error, info, trace, warn};

use flog::FortunateLogger;
use sha2::{ Sha256, Digest };
use tsgen::{get_ts, get_epoch};
use std::{time::{ UNIX_EPOCH, }, collections::HashMap};
use std::env;

use crate::event::EventBuffer;

async fn program() {
  let args: Vec<String> = env::args().collect();
  let component = &args[1];

  println!("{:?}", args);

  if (component == "parse") {
    let op = &args[2];

    if (op == "ev") {
        let buffer = &args[3];
        let eb = EventBuffer { buffer: buffer.to_owned() };
        let result = event::Event::parse_event_buffer(&eb);
        println!("parsed: {:?}", result);
    }
  }

  if (component == "nd") {
    let mut node0 = node::FNode::new(String::from("abd3")).await;
    node0.process().await;
  }

  if (component == "fnz") {
    let op = &args[2];
    let fnz  = finalizer::FortunateNodeSignalFinalizer::new().await;

    if (op == "gf") {
        let epoch = &args[3];

        let fnz  = finalizer::FortunateNodeSignalFinalizer::new().await;
        let blockhash = 
            fnz.finalize_nodesignalblock(
                &String::from(epoch), 
                None,
            )
                .await;
        debug!("{} block completely finalized", epoch);
    }

    else if (op == "f") {
      let epoch =  &args[3];
      let prv_epoch = &args[4];
     
        let blockhash = 
            fnz.finalize_nodesignalblock(
                &String::from(epoch), 
                Some(&String::from(prv_epoch)),
            )
                .await;
        debug!("{} block completely finalized", epoch);
    }

    else if (op == "v") {
        let epoch = &args[3];
        println!("verify nodesignalblock {}", epoch.to_owned());
        let result = fnz.verify_nodesignalblock(epoch).await;
        if (result) {
            println!("Successfully verified {}.", epoch);
        }
        else {
            println!("invalid hash block {}!", epoch);
        }

    }
  }

  else if (component == "ev") {
    println!("started to generate event");
    let epoch = std::string::String::from(&args[2]);

    let mut event_generator = event::PEventGenerator {
        uuid: std::string::String::from("abcd"),
        seed: 0,
        ts: tsgen::get_ts(),
        dynamo_client: crate::dynamoc::get_dynamo_client().await
    };
    let commiter = event::EventCmtr::new().await;

    let res = event_generator.generate_event_pe2(&epoch).await;
    let event = res.unwrap();

    let result = commiter.commit_event(&event).await;
    match result {
        Ok(e) =>  println!("{:?}", event),
        Err(err) => println!("{:?}", err)
    }
    
  }

}


#[tokio::main]
async fn main() {
    program().await;
    let fnz  = finalizer::FortunateNodeSignalFinalizer::new().await;
    return;

    let epoch = String::from("550FC6");
    let prev_blockhash = String::from("35C87A9024A6D92D5760A352870ABBEC3C5A32D14432F66EF1D9142CDD278E9F");
    let signals = fnz.get_node_signals(&epoch).await;
    let fts = String::from("1672653383104798");

    fnz.verify_nodesignalblock(&String::from("550FC6")).await;
    return;

    let block = fnz.build_block(&epoch, &prev_blockhash, &signals, &fts);
    println!("{}", block.buffer);

    let hash = fnz.hash_nodesignalblock(&block);
    println!("{}", hash.hash);

    return;


    let blockhash = 
        fnz.finalize_nodesignalblock(
            &String::from("550FC6"), 
            Some(&String::from("550FC5")), 
        )
            .await;

    return;

    let blockhash = 
        fnz.finalize_nodesignalblock(
            &String::from("550FC6"), 
            Some(&String::from("550FC5")), 
        )
            .await;

    println!("{}", blockhash.unwrap().hash);

    return;

    let signals = 
        fnz.get_node_signals(&String::from("550FC6")).await;

    let ts = String::from("1672646495736701");
    let block = fnz
        .build_block(
            &String::from("550FC6"), 
            &String::from("GENESS"), 
            &signals, 
            &ts);

    let hash = fnz.hash_nodesignalblock(&block);

    println!("{}", hash.hash);
    return;

    let blockhash = 
        fnz.finalize_nodesignalblock(
            &String::from("550FC5"), 
            None, //     Some(&String::from("550FC5"))
        )
            .await;

    println!("{}", blockhash.unwrap().hash);

    return;

    let mut group = sessions::FortunateGroupSession::new();
    group.init_group_session();
    println!("commit");

    FortunateLogger::init();
    
    let mut c = cursor::Cursor::new(String::from("abcdefg"));
    let m0 = c.advance(1);
    let m1 = c.advance(2);
    let m2 = c.advance(3);

    println!("{:?}", c);
    println!("{}", m0);
    println!("{}", m1);
    println!("{}", m2);

    let ts = tsgen::get_ts();
    let ep: String = tsgen::get_epoch();

    println!("{:?}", tsgen::get_ts_pair());
    println!("{:?}", tsgen::get_ts_pair());

    let client = dynamoc::get_dynamo_client().await;
    let res = client.list_tables().send().await;

    dbg!(res.unwrap().table_names);

    let mut rng = rand::thread_rng();

    let n1: u8 = rng.gen();
    let n2: u16 = rng.gen();

    let f1 = 1;
    let f2 = 2;
    let f2 = 4;
    let f3 = 8;
    let f4 = 16;


    //node0.insert_node_signal().await;
}




