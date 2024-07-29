use std::fmt;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, QueryBuilder};
use sqlx::postgres::PgPoolOptions;
use crate::metadata::Metadata;
use crate::source::source::Source;
use crate::utils::get_fqn_table;

pub struct CrateDB {}


impl CrateDB {
    pub(crate) fn get_query_builder_insert<'a>(&self, schema: &'a str, table_name: &'a str, columns: &'a Vec<String>) -> QueryBuilder<'a, Postgres> {
        let stmt = format!("INSERT INTO {} ({})",
                           get_fqn_table(&schema, &table_name),
                           columns.join(","));
        return QueryBuilder::new(stmt);
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
            .connect("")
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

    async fn migrate_table<T: Source>(&self, to: &T) {
        todo!()
    }

    async fn insert_to<T: Source>(&self, schema: &str, table: &str, columns: &Vec<String>, items: &Vec<Self::RowType>, to: &T) {
        todo!()
    }

    async fn migrate_table_to_cratedb(&self, schema: &str, table: &Self::TableType, cratedb: CrateDB, metadata: &mut Metadata) {
        todo!()
    }
}


impl fmt::Debug for CrateDB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TODO: ADD STRUCT DEFINITION")
    }
}
