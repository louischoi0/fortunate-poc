use aws_sdk_dynamodb::{
  model::{
      AttributeDefinition, BillingMode, KeySchemaElement,
      KeyType, ScalarAttributeType, Get,
  },
  Client, Error, client::fluent_builders::{PutItem, Query, GetItem}, 
};
use aws_sdk_dynamodb::{
  model::AttributeValue, 
};

use std::{collections::{HashMap, hash_map::RandomState}, result, borrow::BorrowMut};
use log::{debug, error, info, trace, warn};
use crate::primitives::{DataType, Pair};

pub async fn get_dynamo_client() -> Client {
  let shared_config = aws_config::load_from_env().await;
  Client::new(&shared_config)
}


struct DynamoSchemaColumn {
  pub name: &'static str,
  pub type_name: &'static str,
}

pub enum DynamoSelectQuerySubType {
  All,
  One,
  Page(u64, u64),
}
type DynamoWhereCondition<'a> = Pair<&'a str, DataType>;


pub struct DynamoSelectQueryContext<'a> {
  pub table_name: &'a str,
  // multiple codntions is always evaluated as AND queries.
  pub conditions: Option<std::vec::Vec<DynamoWhereCondition<'a>>>,
  pub query_subtype: DynamoSelectQuerySubType,
}

pub struct DynamoInsertQueryContext<'a> {
  pub table_name: &'a str, 
  pub data: std::vec::Vec<Pair<&'a str, DataType>>,
}

impl <'a> DynamoInsertQueryContext<'a> {
  fn dataasmap(&self) -> HashMap<String, AttributeValue> {
    let mut data = HashMap::<String, AttributeValue>::new();

    for it in self.data.iter() {
      data.insert(
        std::string::String::from(it.k), 
        DynamoHandler::convert_to_dynamo_attributes(&it.v).unwrap()
      );
    }

    data
  }
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

type QuerySetSelectAllResult = Option<Vec<HashMap<String, AttributeValue, RandomState>>>;
type QuerySetGetResult = Option<HashMap<String, AttributeValue, RandomState>>;

pub enum SelectQuerySetResult {
  All(QuerySetSelectAllResult),
  One(QuerySetGetResult)
}

trait DynamoConvertable {
  fn asdtype(&self) -> AttributeValue;
}

impl DynamoConvertable for std::string::String {
  fn asdtype(&self) -> AttributeValue {
    AttributeValue::S(self.to_owned())    
  }
}

impl DynamoConvertable for usize {
  fn asdtype(&self) -> AttributeValue {
    AttributeValue::N(self.to_string())
  }
}

impl DynamoConvertable for u8 {
  fn asdtype(&self) -> AttributeValue {
    AttributeValue::N(self.to_string())
  }
}

impl DynamoConvertable for u16 {
  fn asdtype(&self) -> AttributeValue {
    AttributeValue::N(self.to_string())
  }
}

fn bind_value_to_conditions(q: &Query, cond: &Vec<DynamoWhereCondition>) -> Query {
  let mut ac = q.to_owned();

  for it in cond.iter() {
    let data = DynamoHandler::convert_to_dynamo_attributes(&it.v).unwrap();

    ac = ac.expression_attribute_values(
      format!(":{}", it.k),
      data 
    );
  }
  
  ac
}

fn build_query_string_from_conditions(conditions: &Vec<DynamoWhereCondition>) -> std::string::String {
  let mut qs = std::string::String::from("");

  for c in conditions.iter() {
    let s = format!("{} = :{} and ", c.k, c.k);
    qs += &std::string::String::from(s);
  };

  qs = qs[0..qs.len()-4].to_string();
  qs
}

pub trait DynamoQueriable {
  fn bind_conditions(&self, cond: &Vec<DynamoWhereCondition>) -> Self;
  fn build(client: &Client, qctx: &DynamoSelectQueryContext) -> Self;
}

impl DynamoQueriable for aws_sdk_dynamodb::client::fluent_builders::Query { 

  fn build(client: &Client, qctx: &DynamoSelectQueryContext) -> Self {
    let q = client.query().table_name(qctx.table_name);

    match &qctx.conditions {
      Some(x) => {
        q.bind_conditions(x)
      },
      None => q
    }
  }

  fn bind_conditions(
    &self, 
    conditions: &Vec<DynamoWhereCondition>
  ) -> Self  {
    let qs = build_query_string_from_conditions(conditions);
    let q = self.to_owned();
    let q= q.key_condition_expression(qs);

    bind_value_to_conditions(&q, conditions)
  }
}

impl DynamoQueriable for aws_sdk_dynamodb::client::fluent_builders::GetItem { 

  fn build(client: &Client, qctx: &DynamoSelectQueryContext) -> Self {
    let q = client.get_item().table_name(qctx.table_name);

    match &qctx.conditions {
      Some(x) => {
        q.bind_conditions(x)
      },
      None => q
    }
  }

  fn bind_conditions(
    &self, 
    conditions: &Vec<DynamoWhereCondition>
  ) -> Self {
    // GetItem takes just one condition ahead.
    let first_condition = conditions[0].clone();
    let c = self.to_owned();
    c.key(first_condition.k, DynamoHandler::convert_to_dynamo_attributes(&first_condition.v).unwrap())
  }
}


impl DynamoHandler {

  pub fn convert_to_dynamo_attributes(
    data: &crate::primitives::DataType
  ) -> Result<AttributeValue, ()> {

    match data {
      crate::primitives::DataType::S(x) => Ok(x.asdtype()),
      crate::primitives::DataType::IDX(x) => Ok(x.asdtype()),
      crate::primitives::DataType::U8(x) => Ok(AttributeValue::N(x.to_string())),
      crate::primitives::DataType::U16(x) => Ok(AttributeValue::N(x.to_string())),
      crate::primitives::DataType::U32(x) => Ok(AttributeValue::N(x.to_string())),
      crate::primitives::DataType::U64(x) => Ok(AttributeValue::N(x.to_string())),
    }

  }

  pub fn make_insert_request(&self, client: &Client, data: HashMap<String, String>) -> PutItem {
    let mut h = client.put_item()
      .table_name(self.schema.table_name);

    for (k,v) in data {
      h = h.item(k, AttributeValue::S(String::from(&v)));
    }

    h
  }

  pub async fn put<'a>(
    &self, 
    client: &Client,
    qctx: &DynamoInsertQueryContext<'a>,
  ) -> Result<bool, ()> {
    let mut h = client.put_item().table_name(qctx.table_name);


    Ok(true)
  }

  pub async fn q<'a>(
    &self, 
    client: &Client, 
    qctx: &DynamoSelectQueryContext<'a>,
  ) -> Result<SelectQuerySetResult, ()> {

    match &qctx.query_subtype  {
      DynamoSelectQuerySubType::All => {
        let mut q = aws_sdk_dynamodb::client::fluent_builders::Query::build(client, qctx);
        let rspn = q.send().await;
        Ok((SelectQuerySetResult::All(rspn.unwrap().items)))
      },
      DynamoSelectQuerySubType::One => {
        let mut q = aws_sdk_dynamodb::client::fluent_builders::GetItem::build(client, qctx);
        let rspn = q.send().await;
        Ok((SelectQuerySetResult::One(rspn.unwrap().item)))
      },
      _ => panic!("TODO..")
    }
  }

  pub async fn commit(&self, request: PutItem) -> Result<(), Error> {

    let res = request.send().await;

    match &res {
      Ok(x) => {
        Ok(())
      },
      Err(e) => {
        panic!("SDK ERROR! {:?}", e);
      },
    }
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

