use async_trait::async_trait;
use mongodb::bson::Document;
use sqlx::{Pool, Postgres, Row};
use sqlx::postgres::PgPoolOptions;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::source::Source;

struct PostgresSource {}
pub struct PgTable{}
#[async_trait]
impl Source for PostgresSource {
    type PoolType = Pool<Postgres>;
    type ErrorType = sqlx::Error;
    type ClientType = ();
    type DatabaseType = ();
    type TableType = PgTable;
    type RowType = ();

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

    async fn migrate_table<T: Source>(&self, to: &T) {
        todo!()
    }



    async fn migrate_table_to_cratedb(&self, table: &Self::TableType, cratedb: CrateDB) {
        todo!()
    }
}