mod utils;
mod source;
mod sink;
mod metadata;
mod experiment;

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use source::mongodb::driver::MongoDBSource;
use crate::experiment::data::{CDataFrame, CValue};
use crate::experiment::trans::{iter_cols};
use crate::sink::cratedb::driver::CrateDB;

use crate::metadata::Metadata;
use crate::source::mysql::driver::MySqlSource;
use crate::source::postgres::driver::PostgresSource;
use crate::source::source::Source;


#[tokio::main]
async fn main() {
    let mut metadata = Metadata::new();
    metadata.start();

    let cratedb = CrateDB {};

    let mongodb = MongoDBSource {
        uri: String::from("mongodb://localhost")
    };

    let postgres = PostgresSource {
        uri: "postgres://postgres:postgres@192.168.88.251:5400/postgres".to_string()
    };

    let mysql = MySqlSource {
        uri: "mysql://root:mysql@localhost:3306".to_string(),
    };

    let table = mongodb.get_table("testdb", "unstructured_array").await.unwrap();

    let ignored_columns: Vec<&str> = vec![
        "_id",
        // "id",
        "txt",
        "ip",
        "i16",
        "u16",
        "i32",
        "u32",
        "i32_from_text",
        "f32",
        "f64",
        "bool",
        "null_",
        "datetime",
        "datetime_2",
        "obj",
        "vector_float_simple",
        "vector_float",
        "array_i32",
        "array_u32",
        "array_text",
        "empty_array",
        "array_mixed",
        "empty_multi_arrays",
        "empty_nested_array",
        "empty_nested_array_2",
        "empty_nested_array_3",
        "empty_nested_array_4",
        "empty_nested_array_5",
    ];
    let dataframe = CDataFrame::new();

    iter_cols(&table, &mut metadata).await;

    // mongodb.migrate_table_to_cratedb("doc", &table, ignored_columns, cratedb, &mut metadata).await;
    // mongodb.migrate_table_to_cratedb_pg("doc", &table, ignored_columns, cratedb, &mut metadata).await;
    // postgres.migrate_table_to_cratedb("public", &String::from("simple_array"), ignored_columns, cratedb, &mut metadata).await;
    // mysql.migrate_table_to_cratedb("mysql", &"simple_array".to_string(), ignored_columns, cratedb, &mut metadata).await;
    metadata.print_total_duration();
}