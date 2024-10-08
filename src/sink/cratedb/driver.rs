use std::{fmt};

use async_trait::async_trait;

use mongodb::bson::Document;
use mongodb::Collection;
use serde_json::json;

use sqlx::{Executor, Pool, Postgres, QueryBuilder, Row};
use sqlx::postgres::PgPoolOptions;
use crate::experiment::data::CValue;
use crate::metadata::Metadata;
use crate::source::mongodb::driver::{StringRow};
use crate::source::source::{Sink, Source};
use crate::utils::get_fqn_table;

#[derive(Copy, Clone)]
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
        println!("{:?}", body);

        let client = reqwest::Client::new();
        let res = client.post("http://crate@192.168.88.251:4200/_sql")
            .json(&body)
            .send()
            .await;
        match res {
            Ok(r) => println!("{:?}", r.status()),
            Err(e) => println!("{:?}", e)
        }
    }
    async fn send_batch(&self, schema: &str, table_name: &str, columns: &Vec<String>, buffer: Vec<Vec<CValue>>) {
        let mut query_builder = self.build_insert_values_statement(&schema, &table_name, &columns);

        query_builder.push_values(&buffer, |mut separated, x| {
            for value in x {
                match value {
                    CValue::None => separated.push_bind::<Option<String>>(None),
                    CValue::Bool(v) => separated.push_bind(v),
                    CValue::String(v) => separated.push_bind(v),
                    CValue::I16(v) => separated.push_bind(v),
                    CValue::I32(v) => separated.push_bind(v),
                    CValue::I64(v) => separated.push_bind(v),
                    CValue::Double32(v) => separated.push_bind(v),
                    CValue::Double64(v) => separated.push_bind(v),
                    CValue::VecF32(v) => separated.push_bind(v),
                    CValue::VecF64(v) => separated.push_bind(v),
                    CValue::VecI32(v) => separated.push_bind(v),
                    CValue::VecI64(v) => separated.push_bind(v),
                    CValue::VecString(v) => separated.push_bind(v),
                    _ => separated.push_bind("CVALUE_ERROR_REPORT_CRATEDB")
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
        let pool = self.get_pool().await.unwrap();
        let query = format!(
            "
            SELECT
              table_name
            FROM
              information_schema.tables
            WHERE
              table_schema = '{}'
              ", database
        );
        let result = pool.fetch_all(&*query).await;

        match result {
            Ok(row) => {
                let table_vec: Result<Vec<String>, sqlx::Error> = row.iter().map(|row|row.try_get(0)).collect();
                match table_vec {
                    Ok(result) => Ok(result),
                    _=> panic!("Couldn't deserialize tables")
                }
            },
            Err(_) => Ok(vec![])
        }
    }
    async fn get_database(&self, database: &str) -> Result<Self::DatabaseType, Self::ErrorType> {
        todo!()
    }
    async fn get_table(&self, database: &str, table_name: &str) -> Result<Self::TableType, Self::ErrorType> {
        todo!()
    }

    async fn count(&self, database: &str, table_name: &str) -> Result<i64, Self::ErrorType> {
        let pool = self.get_pool().await.unwrap();
        let query = format!(
            "SELECT COUNT(*) FROM {}.{}", database, table_name
        );
        let result = pool.fetch_one(&*query).await;

        match result {
            Ok(row) => {
                let table_vec: Result<i64, sqlx::Error> = row.try_get(0);
                match table_vec {
                    Ok(result) => Ok(result),
                    Err(e)=> panic!("{}", e)
                }
            },
            Err(_) => Ok(0)
        }
    }

    async fn migrate_table_to_cratedb_pg(&self, schema: &str, table: &Collection<Document>, ignored_columns: Vec<&str>, cratedb: CrateDB, metadata: &mut Metadata) {
        todo!()
    }
    async fn migrate_table_to_cratedb(&self, schema: &str, table: &Self::TableType, ignored_columns: Vec<&str>, cratedb: CrateDB, metadata: &mut Metadata) {
        todo!()
    }
    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<CValue> {
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
