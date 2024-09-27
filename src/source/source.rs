use async_trait::async_trait;
use mongodb::bson::{Document};
use mongodb::Collection;
use sqlx::{Postgres, QueryBuilder};
use crate::experiment::data::CValue;
use crate::metadata::Metadata;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::mongodb::driver::{StringRow};

#[async_trait]
pub trait Source {
    type PoolType;
    type ErrorType;
    type ClientType;
    type DatabaseType;
    type TableType;
    type RowType;

    async fn get_pool(&self) -> Result<Self::PoolType, Self::ErrorType>;
    async fn get_client(&self) -> Result<Self::ClientType, Self::ErrorType>;
    async fn list_databases(&self) -> Result<Vec<String>, Self::ErrorType>;
    async fn list_tables(&self, database: &str) -> Result<Vec<String>, Self::ErrorType>;
    async fn get_database(&self, database: &str) -> Result<Self::DatabaseType, Self::ErrorType>;
    async fn get_table(&self, database: &str, table_name: &str) -> Result<Self::TableType, Self::ErrorType>;

    async fn count(&self, database: &str, table_name: &str) -> Result<i64, Self::ErrorType>;
    async fn migrate_table_to_cratedb_pg(&self, schema: &str, table: &Collection<Document>, ignored_columns: Vec<&str>, cratedb: CrateDB, metadata: &mut Metadata);
    async fn migrate_table_to_cratedb(&self, schema: &str, table: &Self::TableType, ignored_columns: Vec<&str>, cratedb: CrateDB, metadata: &mut Metadata);
    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<CValue>;
    fn row_to_vec_str(&self, row: Self::RowType) -> Vec<StringRow>;
}

#[async_trait]
pub trait Sink {
    fn build_insert_values_statement(&self, schema: &str, table_name: &str, columns: &Vec<String>) -> QueryBuilder<Postgres>;
    fn get_bulk_args_query(&self, schema: &str, table_name: &str, columns: &Vec<String>) -> String;
    async fn send_batch_http(&self, schema: &str, table_name: &str, columns: &Vec<String>, buffer: Vec<Vec<StringRow>>);
    async fn send_batch(&self, schema: &str, table_name: &str, columns: &Vec<String>,buffer: Vec<Vec<CValue>>);
}
