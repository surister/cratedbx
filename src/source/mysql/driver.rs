use std::collections::HashSet;
use std::net::IpAddr;
use std::time::Duration;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::StreamExt;
use mongodb::bson::Bson::Array;
use mongodb::bson::{Bson, Document};
use mongodb::Collection;
use serde_json::Value;
use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::{Column, MySqlPool, Row, TypeInfo};
use sqlx::encode::IsNull::No;
use crate::metadata::Metadata;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::mongodb::driver::{NormalizedRow, StringRow};
use crate::source::source::{Sink, Source};
macro_rules! mysql2normalized {
    ($row:expr, $name:expr, $type_name:ty, $result_type:expr) => {
        {
            let val: Result<$type_name, sqlx::Error> = $row.try_get($name);
            let result: NormalizedRow;

                match val {
                    Ok(v) => {
                        result = $result_type(v);
                    }
                    Err(_) => result = NormalizedRow::None
                }

            result
        }

    };
        ($row:expr, $name:expr, $type_name:ty, $result_type:expr, $to_string:expr) => {
        {
            let val: Result<$type_name, sqlx::Error> = $row.try_get($name);
            let result: NormalizedRow;

                match val {
                    Ok(v) => {
                        result = $result_type($to_string(v));
                    }
                    Err(_) => result = NormalizedRow::None
                }

            result
        }

    };
}
pub struct MySqlSource {
    pub uri: String,
}

#[async_trait]
impl Source for MySqlSource {
    type PoolType = MySqlPool;
    type ErrorType = sqlx::Error;
    type ClientType = ();
    type DatabaseType = ();
    type TableType = String;
    type RowType = MySqlRow;

    async fn get_pool(&self) -> Result<Self::PoolType, Self::ErrorType> {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::new(5, 0))
            .connect(&*self.uri)
            .await?;
        Ok(pool)
    }

    async fn get_client(&self) -> Result<Self::ClientType, Self::ErrorType> {
        todo!()
    }

    async fn list_databases(&self) -> Result<Vec<String>, Self::ErrorType> {
        todo!()
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, Self::ErrorType> {
        todo!()
    }

    async fn get_database(&self, database: &str) -> Result<Self::DatabaseType, Self::ErrorType> {
        todo!()
    }

    async fn get_table(&self, database: &str, table_name: &str) -> Result<Self::TableType, Self::ErrorType> {
        todo!()
    }

    async fn migrate_table_to_cratedb_pg(&self, schema: &str, table: &Collection<Document>, ignored_columns: Vec<&str>, cratedb: CrateDB, metadata: &mut Metadata) {
        todo!()
    }

    async fn migrate_table_to_cratedb(&self, schema: &str, table: &Self::TableType, ignored_columns: Vec<&str>, cratedb: CrateDB, metadata: &mut Metadata) {
        let pool = self.get_pool().await.unwrap();
        if !ignored_columns.is_empty() {
            let mut columns: Vec<String> = vec![];
            let query_limit = format!("SELECT * FROM {}.{} LIMIT 1", schema, table);
            let one_row = sqlx::query(&*query_limit).fetch_one(&pool).await;
            let mut total_documents_sent = 0;
            let batch_size = 1000;

            let mut buffer: Vec<Vec<NormalizedRow>> = vec![];

            match one_row {
                Ok(row) => {
                    let one_row_columns: Vec<String> = row.columns().iter().map(|x| String::from(x.name())).collect();
                    let set1: HashSet<String> = one_row_columns.into_iter().collect();
                    let set2: HashSet<String> = ignored_columns.into_iter().map(|i| String::from(i)).collect();

                    let difference: HashSet<String> = set1.difference(&set2).cloned().collect();
                    columns = difference.into_iter().collect();
                    println!("{:?}", columns)
                }
                Err(e) => panic!("{}", format!("{} - Could not get a LIMIT one row to ignore columns. Is there data in the table?", e))
            }
            let query_1 = format!("SELECT {} FROM {}.{}", &columns.join(","), &schema, &table);
            let mut rows_stream = sqlx::query(&query_1).fetch(&pool);

            metadata.print_step("Starting connections");

            while let Some(row) = rows_stream.next().await {
                let mut normalized_row: Vec<NormalizedRow> = vec![];
                let mut columns: Vec<String> = vec![];

                match row {
                    Ok(row_obj) => {
                        columns = row_obj.columns().iter().map(|x| String::from(x.name())).collect();
                        normalized_row = self.row_to_normalized_row(row_obj).await;
                    }
                    Err(e) => {}
                }

                let buffer_len = &buffer.len();

                if buffer_len == &batch_size {
                    cratedb.send_batch("doc", &table, &columns, buffer).await;
                    metadata.print_step(format!("Sent batch of {}", buffer_len).as_str());
                    total_documents_sent += buffer_len;
                    buffer = vec![];
                }
                buffer.push(normalized_row);
            }
            metadata.print_step(format!("Total records sent: {}", total_documents_sent).as_str());
            metadata.print_step(format!("Rows per seconds: {}", (total_documents_sent as u128) / metadata.elapsed().as_millis() * 1000).as_str());
        }
    }

    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<NormalizedRow> {
        let mut new_row = vec![];
        for col in row.columns() {
            let name = col.name();
            let type_info = col.type_info();
            let type_name = type_info.name();

            match type_name {
                "TEXT" => {
                    new_row.push(mysql2normalized!(row, name, String, NormalizedRow::String))
                }
                "INET" => {
                    new_row.push(mysql2normalized!(row, name, IpAddr, NormalizedRow::String, |v:IpAddr|v.to_string()))
                }
                "SMALLINT" => {
                    new_row.push(mysql2normalized!(row, name, i16, NormalizedRow::I16))
                }
                "INT" => {
                    new_row.push(mysql2normalized!(row, name, i32, NormalizedRow::I32))
                }
                "DOUBLE" => {
                    new_row.push(mysql2normalized!(row, name, f32, NormalizedRow::Double32))
                }
                "BOOLEAN" => {
                    new_row.push(mysql2normalized!(row, name, bool, NormalizedRow::Bool))
                }
                "DATETIME" => {
                    new_row.push(mysql2normalized!(row, name, NaiveDateTime, NormalizedRow::String, |v: NaiveDateTime|v.to_string()));
                }
                "JSON" => {
                    let val: Value = row.try_get(name).unwrap();
                    match val {
                        Value::Array(arr) => {
                            let first = arr.first();
                            match first {
                                // We check the first value of the array, and depending on the type
                                // we construct a different thing.
                                Some(v) => {
                                    match v {
                                        Value::Number(_) => {
                                            let _v: Vec<f64> = arr.into_iter().map(|x| x.as_f64().unwrap()).collect();
                                            new_row.push(NormalizedRow::VecF64(_v))
                                        }
                                        Value::String(_) => {
                                            let _v: Vec<String> = arr.into_iter().map(|x| {
                                                match x {
                                                    Value::String(s) => s,
                                                    _ => x.to_string()
                                                }
                                            }).collect::<Vec<String>>();
                                            new_row.push(NormalizedRow::VecString(_v))
                                        }
                                        _ => ()
                                    }
                                }
                                None => {
                                    new_row.push(NormalizedRow::None)
                                }
                            }
                        }
                        _ => {
                            new_row.push(mysql2normalized!(row, name, Value, NormalizedRow::String, |v:Value|v.to_string()))
                        }
                    }
                }

                _ => {
                    let val = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::String(v)),
                        Err(e) => panic!("Datatype is {} - {} ", type_name, e)
                    }
                }
            };
        };
        return new_row;
    }

    fn row_to_vec_str(&self, row: Self::RowType) -> Vec<StringRow> {
        todo!()
    }
}