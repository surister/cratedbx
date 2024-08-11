use std::fmt;
use async_trait::async_trait;
use mongodb::bson::Document;
use mongodb::Collection;
use serde_json::json;
use sqlx::{Pool, Postgres, QueryBuilder};
use sqlx::postgres::PgPoolOptions;
use crate::metadata::Metadata;
use crate::source::mongodb::driver::{NormalizedRow, StringRow};
use crate::source::source::{Sink, Source};
use crate::utils::get_fqn_table;

pub struct CrateDB {}

#[async_trait]
impl Sink for CrateDB {
    fn build_insert_values_statement(&self, schema: &str, table_name: &str, columns: &Vec<String>) -> QueryBuilder<Postgres> {
        let stmt = format!("INSERT INTO {} ({}) ",
                           get_fqn_table(&schema, &table_name),
                           columns.join(","));
        return QueryBuilder::new(stmt);
    }

    fn get_bulk_args_query(&self, schema: &str, table_name: &str, columns: &Vec<String>) -> String {
        let mut interpolation = "?,".repeat(columns.len() - 1);
        interpolation.push('?');

        let stmt = format!("INSERT INTO {} ({}) VALUES ({})",
                           get_fqn_table(&schema, &table_name),
                           columns.join(","),
                            interpolation
        );
        stmt
    }

    async fn send_batch_http(&self, schema: &str, table_name: &str, columns: &Vec<String>, buffer: Vec<Vec<StringRow>>) {
        let query = self.get_bulk_args_query(&schema, &table_name, &columns);

        let body = json!({
           "stmt": query, "bulk_args": &buffer
        });

        let client = reqwest::Client::new();
        let res = client.post("http://crate@192.168.88.251:4200/_sql")
            .json(&body)
            .send()
            .await;
        match res {
            Ok(r) => (),//println!("{:?}", r.text().await),
            Err(e) => println!("{:?}", e)
        }
    }
    async fn send_batch(&self, schema: &str, table_name: &str, columns: &Vec<String>, buffer: Vec<Vec<NormalizedRow>>) {
        let mut query_builder = self.build_insert_values_statement(&schema, &table_name, &columns);

        query_builder.push_values(&buffer, |mut separated, x| {
            for value in x {
                match value {
                    NormalizedRow::None => separated.push_bind::<Option<String>>(None),
                    NormalizedRow::Bool(v) => separated.push_bind(v),
                    NormalizedRow::String(v) => separated.push_bind(v),
                    NormalizedRow::I16(v) => separated.push_bind(v),
                    NormalizedRow::I32(v) => separated.push_bind(v),
                    NormalizedRow::I64(v) => separated.push_bind(v),
                    NormalizedRow::Double32(v) => separated.push_bind(v),
                    NormalizedRow::Double64(v) => separated.push_bind(v),
                    NormalizedRow::VecF32(v) => separated.push_bind(v),
                    NormalizedRow::VecF64(v) => separated.push_bind(v),
                    NormalizedRow::VecI32(v) => separated.push_bind(v),
                    NormalizedRow::VecI64(v) => separated.push_bind(v),
                    NormalizedRow::VecString(v) => separated.push_bind(v),


                    _ => continue
                };

            }
        });
        let pool = self.get_pool().await.expect("Couldn't connect to CrateDB");
        query_builder.build().execute(&pool).await.expect("Could not send batch");
    }   
}

#[async_trait]
impl Source for CrateDB {
    type PoolType = Pool<Postgres>;
    type ErrorType = sqlx::Error;
    type ClientType = ();
    type DatabaseType = ();
    type TableType = ();
    type RowType = ();

    async fn get_pool(&self) -> Result<Self::PoolType, Self::ErrorType> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgresql://crate:crate@192.168.88.251:5432")
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
        todo!()
    }

    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<NormalizedRow> {
        todo!()
    }

    fn row_to_vec_str(&self, row: Self::RowType) -> Vec<StringRow> {
        todo!()
    }
}


impl fmt::Debug for CrateDB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TODO: ADD STRUCT DEFINITION")
    }
}
