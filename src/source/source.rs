use async_trait::async_trait;
use crate::sink::cratedb::driver::CrateDB;

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
    async fn insert_to<T: Source>(&self, schema: &str, table: &str, items: &Vec<Self::RowType>, to: &T) {}
    async fn migrate_table_to_cratedb(&self, table: &Self::TableType, cratedb: CrateDB);
}