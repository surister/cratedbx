use std::collections::HashSet;
use std::net::IpAddr;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::StreamExt;
use mongodb::bson::Document;
use mongodb::Collection;
use sqlx::{Column, Error, Pool, Postgres, Row, TypeInfo};

use sqlx::postgres::{PgPoolOptions, PgRow};

use crate::metadata::Metadata;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::mongodb::driver::{NormalizedRow, StringRow};
use crate::source::source::{Sink, Source};

macro_rules! pg2normalized {
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
            let val: Result<$type_name, Error> = $row.try_get($name);
            let result: NormalizedRow;

                match val {
                    Ok(v) => {
                        result = $result_type($to_string(v));
                    },
                    Err(_) => result = NormalizedRow::None
                }

            result
        }

    };
}

pub struct PostgresSource {
    pub(crate) uri: String
}

#[async_trait]
impl Source for PostgresSource {
    type PoolType = Pool<Postgres>;
    type ErrorType = sqlx::Error;
    type ClientType = ();
    type DatabaseType = ();
    type TableType = String;
    type RowType = PgRow;

    async fn get_pool(&self) -> Result<Self::PoolType, Self::ErrorType> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&*self.uri)
            .await?;
        Ok(pool)
    }

    async fn get_client(&self) -> Result<Self::ClientType, Self::ErrorType> {
        todo!()
    }
    async fn list_databases(&self) -> Result<Vec<String>, Self::ErrorType> {
        println!("Listing databases");
        let pool: Self::PoolType = self.get_pool().await.unwrap();
        let result = sqlx::query("SELECT datname as name FROM pg_database WHERE datistemplate = false").fetch_all(&pool).await;
        match result {
            Ok(result) => {
                let tables: Vec<String> = result.iter().map(|row| row.try_get("name").expect("Column not found, does the query return a column called name")).collect();
                return Ok(tables);
            }
            Err(e) => {
                println!("Error - {:?}", e);
                Err(e)
            }
        }
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
        let mut columns: Vec<String> = vec![];
        let mut buffer: Vec<Vec<NormalizedRow>> = vec![];
        let batch_size = 5000;
        let mut total_documents_sent = 0;

        // Compute the difference between the table's column and `ignored_columns`, we only want to
        // select the actual data the user wants.
        if !ignored_columns.is_empty() {
            let one_row_query = format!("SELECT * FROM {}.{} LIMIT 1", &schema, &table);
            let one_row = sqlx::query(&*one_row_query).fetch_one(&pool).await;
            match one_row {
                Ok(row) => {
                    let one_row_columns: Vec<String> = row.columns().iter().map(|x| String::from(x.name())).collect();
                    let set1: HashSet<String> = one_row_columns.into_iter().collect();
                    let set2: HashSet<String> = ignored_columns.into_iter().map(|i| String::from(i)).collect();

                    let difference: HashSet<String> = set1.difference(&set2).cloned().collect();
                    columns = difference.into_iter().collect();
                },
                Err(e) => panic!("{}", format!("{} - Could not get a LIMIT one row to ignore columns. Is there data in the table?", e))
            }
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

    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<NormalizedRow> {
        let mut new_row: Vec<NormalizedRow> = vec![];
        for col in row.columns() {
            let name = col.name();
            let type_info = col.type_info();
            let type_name = type_info.name();

            match type_name {
                "TEXT" => {
                    new_row.push(pg2normalized!(row, name, String, NormalizedRow::String))
                }
                "INET" => {
                    new_row.push(pg2normalized!(row, name, IpAddr, NormalizedRow::String, |v:IpAddr|v.to_string()))
                }
                "INT2" => {
                    new_row.push(pg2normalized!(row, name, i16, NormalizedRow::I16))
                }
                "INT4" => {
                    new_row.push(pg2normalized!(row, name, i32, NormalizedRow::I32))
                }
                "FLOAT4" => {
                    new_row.push(pg2normalized!(row, name, f32, NormalizedRow::Double32))
                }
                "FLOAT8" => {
                    new_row.push(pg2normalized!(row, name, f64, NormalizedRow::Double64))
                }
                "BOOL" => {
                    new_row.push(pg2normalized!(row, name, bool, NormalizedRow::Bool))
                }
                "TIMESTAMP" => {
                    new_row.push(pg2normalized!(row, name, NaiveDateTime, NormalizedRow::String, |v: NaiveDateTime|v.to_string()));
                }
                "TEXT[]" => {
                    new_row.push(pg2normalized!(row, name, Vec<String>, NormalizedRow::VecString));
                }
                "INT4[]" => {
                    new_row.push(pg2normalized!(row, name, Vec<i32>, NormalizedRow::VecI32));
                }
                "INT8[]" => {
                    new_row.push(pg2normalized!(row, name, Vec<i64>, NormalizedRow::VecI64));
                }
                "FLOAT4[]" => {
                    new_row.push(pg2normalized!(row, name, Vec<f32>, NormalizedRow::VecF32));
                }
                "FLOAT8[]" => {
                    new_row.push(pg2normalized!(row, name, Vec<f64>, NormalizedRow::VecF64))
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