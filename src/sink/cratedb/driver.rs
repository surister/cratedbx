use std::fmt;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, QueryBuilder};
use sqlx::postgres::PgPoolOptions;
use crate::source::source::Source;
use crate::utils::get_fqn_table;

pub struct CrateDB {
    pub(crate) test: u32,
}



impl CrateDB {
    pub(crate) fn get_query_builder_insert<'a>(&self, schema: &'a str, table_name: &'a str, columns: Vec<String>) -> QueryBuilder<'a, Postgres> {
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
    type TableType = crate::source::postgres::driver::PgTable;
    type RowType = ();

    async fn get_pool(&self) -> Result<Self::PoolType, Self::ErrorType> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://crate:crate@192.168.88.251:5432/postgres")
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

    async fn migrate_table_to_cratedb(&self, table: &Self::TableType, cratedb: CrateDB) {
        todo!()
    }
}


impl fmt::Debug for CrateDB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Hi")
    }
}
