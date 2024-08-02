use async_trait::async_trait;
use sqlx::{Postgres, QueryBuilder};
use crate::metadata::Metadata;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::mongodb::driver::NormalizedRow;

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
    async fn migrate_table<T: Source>(&self, to: &T);
    async fn insert_to<T: Source>(&self, schema: &str, table: &str, columns: &Vec<String>, items: &Vec<Self::RowType>, to: &T);
    async fn build_query<T: Sink>(&self, schema: &str, table: &str, columns: &Vec<String>, items: &Vec<Self::RowType>, sink: &T);
    async fn migrate_table_to_cratedb(&self, schema: &str, table: &Self::TableType, cratedb: CrateDB, metadata: &mut Metadata);
    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<crate::source::mongodb::driver::NormalizedRow>;
}

#[async_trait]
pub trait Sink {
    fn build_insert_values_statement(&self, schema: &str, table_name: &str) -> QueryBuilder<Postgres>;
    async fn send_batch(&self, schema: &str, table_name: &str, buffer: Vec<Vec<NormalizedRow>>);
}
