use aws_sdk_dynamodb::model::AttributeValue;
use rand::Rng;

extern crate log4rs;

mod node;
mod fnode;
mod dynamoc;
mod tsgen;
mod flog;
mod cursor;
mod sessions;
mod finalizer;
mod block;
mod event;
mod algorithms;
mod hashlib;
mod payload;
mod primitives;
mod client;
mod matrix;
mod window;
mod actionplanner;
mod eventgenerator;
mod macros;

use log::{debug, error, info, trace, warn};

use flog::FortunateLogger;
use sha2::{ Sha256, Digest };
use tsgen::{get_ts, get_epoch};
use crate::cursor::{TCursor, Cursor};

use std::{time::{ UNIX_EPOCH, }, collections::HashMap};
use std::env;

use crate::{event::EventBuffer, finalizer::FortunateEventFinalizer};
use crate::primitives::{DataType, Pair};
use crate::dynamoc::{ DynamoSelectQueryContext, DynamoSelectQuerySubType };
use tokio::time::{sleep, Duration};

async fn program() {
  let args: Vec<String> = env::args().collect();
  let component = &args[1];

  let a: &str = &std::string::String::from("abcd").as_str();

  info!("args: {:?}", args);

  if (component == "parse") {
    let op = &args[2];

    if (op == "ev") {
        let buffer = &args[3];
        let event_key = buffer.as_str()[0..6+16+16].to_string();

        let eb = EventBuffer { event_key:event_key, buffer: buffer.to_owned() };
        let result = event::Event::parse_event_buffer(&eb);
        info!("parsed: {:?}", result);
    }
  }

  if (component == "fnz") {
    let op = &args[2];
    let region = std::string::String::from("northeast-1");
    let fnz  = 
        finalizer::FortunateNodeSignalFinalizer::new(&region).await;

    if (op == "gf") {
        let epoch = &args[3];
        let region = std::string::String::from("northeast-1");

        let fnz  = 
            finalizer::FortunateNodeSignalFinalizer::new(&region).await;

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
        let result = fnz.verify_nodesignalblock(epoch).await;

        info!("verify nodesignalblock {}", epoch.to_owned());

        if (result) {
            info!("Successfully verified {}.", epoch);
        }
        else {
            info!("invalid hash block {}!", epoch);
        }
    }
  }

  else if (component == "ve") {
    let event_key = &args[2];
    let payload = " \
            'token': 'asdfasfd1223rwefwefewf',
            'nonce': 'asdfdsfsf',
        ";

    let ef = FortunateEventFinalizer::new(
      &std::string::String::from("northeast-1")
    ).await;

    let p = String::from(payload);
    let epoch = "551435".to_string();

    let r = ef.verify_event_payload(&epoch, &event_key,& p).await;
  }

  else if (component == "ev") {
    info!("started to generate event");
    let epoch = std::string::String::from(&args[2]);
    let payload = std::string::String::from("token:asdfasfd1223rwefwefewf;nonce:asdfdsfsf");

    let mut event_generator = event::PEventGenerator::new(
        &std::string::String::from("matrix-0")
    ).await;

    let commiter = event::EventCmtr::new().await;

    let res = event_generator.generate_event_pe2(&payload).await;
    let event = res.unwrap();

    let result = commiter.commit_event(&event).await;

    match result {
        Ok(e) =>  info!("{:?}", event),
        Err(err) => error!("{:?}", err)
    }
    
  }

}


#[tokio::main]
async fn main() {
    FortunateLogger::init();
    crate::client::client_main().await;
    return;

    let a = std::string::String::from("a");
    
    let eh = dynamoc::DynamoHandler::event();
    let c = dynamoc::get_dynamo_client().await;

    let qc = DynamoSelectQueryContext {
        table_name: &"node_signals",
        conditions: Some(vec![
                primitives::Pair::<&'static str, DataType> {
                    k: "epoch",
                    v: DataType::S(std::string::String::from("551435"))
                },
                primitives::Pair::<&'static str, DataType> {
                    k: "signal_key",
                    v: DataType::S(std::string::String::from("5514351672719914600569"))
                },
        ]),
        query_subtype: DynamoSelectQuerySubType::All
    };

    let result = eh.q(&c, &qc).await.unwrap();

    match result {
        dynamoc::SelectQuerySetResult::All(x) => println!("{:?}",x.unwrap()),
        _ => ()
    };

    return;

    program().await;
    return;

    let region = std::string::String::from("northeast-1");
    let fnz  
        = finalizer::FortunateNodeSignalFinalizer::new(&region).await;

    return;

    let epoch = String::from("550FC6");
    let prev_blockhash = String::from("35C87A9024A6D92D5760A352870ABBEC3C5A32D14432F66EF1D9142CDD278E9F");
    let signals = fnz.get_node_signals(&epoch).await;
    let fts = String::from("1672653383104798");

    fnz.verify_nodesignalblock(&String::from("550FC6")).await;
    return;

    let block = fnz.build_block(&epoch, &prev_blockhash, &signals, &fts);
    info!("{}", block.buffer);

    let hash = fnz.hash_nodesignalblock(&block);
    info!("{}", hash.hash);

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

    return;

    let blockhash = 
        fnz.finalize_nodesignalblock(
            &String::from("550FC5"), 
            None, //     Some(&String::from("550FC5"))
        )
            .await;


    return;

    FortunateLogger::init();
    
    let mut c = cursor::Cursor::new(&String::from("abcdefg"));
    let m0 = c.advance(1);
    let m1 = c.advance(2);
    let m2 = c.advance(3);

    let ep: String = tsgen::get_epoch();

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




