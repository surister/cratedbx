use async_trait::async_trait;
use futures::StreamExt;
use sqlx::{Column, Error, Execute, Pool, Postgres, Row, TypeInfo};
use sqlx::postgres::{PgPoolOptions, PgRow};

use crate::metadata::Metadata;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::mongodb::driver::NormalizedRow;
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
    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<NormalizedRow> {
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

    async fn migrate_table<T: Source>(&self, to: &T) {
        todo!()
    }

    async fn insert_to<T: Source>(&self, schema: &str, table: &str, columns: &Vec<String>, items: &Vec<Self::RowType>, to: &T) {
        todo!()
    }

    async fn build_query<T: Sink>(&self, schema: &str, table: &str, columns: &Vec<String>, items: &Vec<Self::RowType>, sink: &T) {
        // let mut query_builder = sink.get_query_builder_insert("doc", &table, &columns).await;
        todo!()
    }


    async fn migrate_table_to_cratedb(&self, schema: &str, table: &Self::TableType, cratedb: CrateDB, metadata: &mut Metadata) {
        let pool = self.get_pool().await.unwrap();
        let cratedb_pool = cratedb.get_pool().await.unwrap();
        let mut total_documents_sent = 0;

        let query_1 = format!("SELECT * FROM {}.{}", &schema, &table);
        let mut rows_stream = sqlx::query(&query_1).fetch(&pool);
        let mut buffer: Vec<PgRow> = vec![];

        metadata.print_step("Starting connections");
        while let Some(row) = rows_stream.next().await {
            match row {
                Ok(row_obj) => {
                    let batch_size = 5000;
                    let columns = &row_obj.columns().iter().map(|x| String::from(x.name())).collect();

                    if &buffer.len() == &batch_size {
                        let query = self.build_query(&schema, &table, &columns, &buffer, &cratedb).await;
                        // query_builder.push_values(
                        //     &buffer, |mut b, rr: &PgRow| {
                        //         for col in rr.columns() {
                        //             let name = col.name();
                        //             let type_info = col.type_info();
                        //             let type_name = type_info.name();
                        //
                        //             match col.type_info().name() {
                        //                 "TEXT" => {
                        //                     let val: Result<String, Error> = rr.try_get(col.name());
                        //                     match val {
                        //                         Ok(v) => b.push_bind(v),
                        //                         Err(e) => b.push_bind::<Option<String>>(None)
                        //                     }
                        //                 }
                        //                 _ => continue
                        //             };
                        //         };
                        //     },
                        // );
                        //
                        // query_builder.build().execute(&cratedb_pool).await.expect("TODO: panic message");
                        metadata.print_step(format!("Sent batch of {}", &buffer.len()).as_str());
                        total_documents_sent += &buffer.len();
                        buffer.clear();
                    }
                    buffer.push(row_obj);
                }
                _ => {}
            }
        }
        metadata.print_step(format!("Total records sent: {}", total_documents_sent).as_str());
        metadata.print_step(format!("Rows per seconds: {}", (total_documents_sent as u128) / metadata.elapsed().as_millis() * 1000).as_str());
    }
}