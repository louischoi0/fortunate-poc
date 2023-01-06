use aws_sdk_dynamodb::{
  model::{
      AttributeDefinition, BillingMode, KeySchemaElement,
      KeyType, ScalarAttributeType,
  },
  Client, Error, client::fluent_builders::PutItem, 
};
use aws_sdk_dynamodb::{
  model::AttributeValue, 
};

use std::{collections::{HashMap, hash_map::RandomState}, result};

pub async fn get_dynamo_client() -> Client {
  let shared_config = aws_config::load_from_env().await;
  Client::new(&shared_config)
}

struct DynamoSchemaColumn {
  name: &'static str,
  type_name: &'static str,
}

impl DynamoSchemaColumn {
  pub fn new(name: &'static str, type_name: &'static str) -> Self {
    DynamoSchemaColumn {
      name: name,
      type_name: type_name,
    }
  }
}

pub struct DynamoSchema {
  table_name: &'static str,
  partition_key: &'static str,
  sort_key: &'static str,
  columns: Vec<DynamoSchemaColumn>,
}

pub struct DynamoHandler {
  schema: DynamoSchema
}

const NODE_SIGANAL_SCHEMA: DynamoSchema = DynamoSchema {
  table_name: "node_signals",
  partition_key: "epoch",
  sort_key: "timestamp",
  columns: vec![
  ],
};

const NODE_SIGANAL_BLOCK_SCHEMA: DynamoSchema = DynamoSchema {
  table_name: "node_signal_blocks",
  partition_key: "epoch",
  sort_key: "timestamp",
  columns: vec![ ],
};

const EVENT_SCHEMA: DynamoSchema = DynamoSchema {
  table_name: "events",
  partition_key: "epoch",
  sort_key: "event_key",
  columns: vec![ ],
};

type QuerySetResult = Option<Vec<HashMap<String, AttributeValue, RandomState>>>;

impl DynamoHandler {
  pub fn make_insert_request(&self, client: &Client, data: HashMap<String, String>) -> PutItem {
    let mut h = client.put_item()
      .table_name(self.schema.table_name);

    for (k,v) in data {
      h = h.item(k, AttributeValue::S(String::from(&v)));
    }

    h
  }

  pub async fn query(&self, client: &Client, query_set: HashMap<String,String>) -> Result<QuerySetResult, Error> {

    let results = client 
      .query()
      .table_name(query_set.get("table_name").unwrap().as_str())
      .key_condition_expression(
        "#key = :key"
        //format!("#key = :{}", ":key")
      )
      .expression_attribute_names(
        "#key", query_set.get("key_column").unwrap()
      )
      .expression_attribute_values(
        ":key", 
        AttributeValue::S(query_set.get("query_value").unwrap().to_owned())
      )
      .send()
      .await;
      
    match results.as_ref() {
      Err(err) => {
        println!("{:?}", err);
      },
      Ok(r) => {
      }
    };

    Ok(results.unwrap().items)
  }

  pub async fn commit(&self, request: PutItem) -> Result<(), Error> {
    request.send().await?;
    Ok(())
  }

  pub fn new(schema: DynamoSchema) -> Self {
    DynamoHandler {
      schema: schema
    }
  }

  pub fn node() -> Self {
    Self::new(NODE_SIGANAL_SCHEMA)
  }

  pub fn nodesignal() -> Self {
    Self::new(NODE_SIGANAL_SCHEMA)
  }

  pub fn nodesignalblock() -> Self {
    Self::new(NODE_SIGANAL_BLOCK_SCHEMA)
  }

  pub fn event() -> Self {
    Self::new(EVENT_SCHEMA)
  }

}
struct DynamoNodeHdr {

}


