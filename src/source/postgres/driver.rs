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

pub struct PostgresSource {}

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
            .connect("postgres://postgres:postgres@192.168.88.251:5400/postgres")
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

        if !ignored_columns.is_empty(){
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

        let mut buffer: Vec<Vec<NormalizedRow>> = vec![];
        let batch_size = 5000;
        let mut total_documents_sent = 0;

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
                    let val: Result<String, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::String(v)),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }
                "INET" => {
                    let val: Result<IpAddr, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::String(v.to_string())),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }
                "INT2" => {
                    let val: Result<i16, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::I16(v)),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "INT4" => {
                    let val: Result<i32, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::I32(v)),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "FLOAT4" => {
                    let val: Result<f32, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::Double32(v)),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }
                "FLOAT8" => {
                    let val: Result<f64, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::Double64(v)),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "BOOL" => {
                    let val: Result<bool, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::Bool(v)),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }
                "TIMESTAMP" => {
                    let val: Result<NaiveDateTime, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::String(v.to_string())),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "TIMESTAMPTZ" => {
                    let val: Result<DateTime<Utc>, Error> = row.try_get(name);
                    match val {
                        Ok(v) => new_row.push(NormalizedRow::String(v.to_rfc3339())),
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "TEXT[]" => {
                    let val: Result<Vec<String>, Error> = row.try_get(name);
                    match val {
                        Ok(v) => {
                            new_row.push(NormalizedRow::VecString(v))
                        },
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "INT4[]" => {
                    let val: Result<Vec<i32>, Error> = row.try_get(name);
                    match val {
                        Ok(v) => {
                            new_row.push(NormalizedRow::VecI32(v))
                        },
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "INT8[]" => {
                    let val: Result<Vec<i64>, Error> = row.try_get(name);
                    match val {
                        Ok(v) => {
                            new_row.push(NormalizedRow::VecI64(v))
                        },
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "FLOAT4[]" => {
                    let val: Result<Vec<f32>, Error> = row.try_get(name);
                    match val {
                        Ok(v) => {
                            new_row.push(NormalizedRow::VecF32(v))
                        },
                        Err(e) => new_row.push(NormalizedRow::None)
                    }
                }

                "FLOAT8[]" => {
                    let val: Result<Vec<f64>, Error> = row.try_get(name);
                    match val {
                        Ok(v) => {
                            new_row.push(NormalizedRow::VecF64(v))
                        },
                        Err(e) => new_row.push(NormalizedRow::None)
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