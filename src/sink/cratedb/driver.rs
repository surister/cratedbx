use std::fmt;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, QueryBuilder};
use sqlx::postgres::PgPoolOptions;
use crate::metadata::Metadata;
use crate::source::mongodb::driver::NormalizedRow;
use crate::source::source::{Sink, Source};
use crate::utils::get_fqn_table;

pub struct CrateDB {}

enum InsertStrategy {
    PgSingleInserts,
    PgValuesInserts,
    HttpBulkArgs,
}
#[async_trait]
impl Sink for CrateDB {
    fn build_insert_values_statement(&self, schema: &str, table_name: &str) -> QueryBuilder<Postgres> {
        let stmt = format!("INSERT INTO {} ",
                           get_fqn_table(&schema, &table_name),
                           );
        return QueryBuilder::new(stmt);
    }

    async fn send_batch(&self, schema: &str, table_name: &str, buffer: Vec<Vec<NormalizedRow>>) {
        let mut query_builder = self.build_insert_values_statement(&schema, &table_name);

        query_builder.push_values(&buffer, |mut separated, x| {
            for value in x {
                match value {
                    NormalizedRow::String(v) => separated.push_bind(v),
                    NormalizedRow::Double64(v) => separated.push_bind(v),
                    NormalizedRow::Double32(v) => separated.push_bind(v),
                    _ => continue
                };

            }
        });
        let pool = self.get_pool().await.expect("Couldn't connect to CrateDB");
        
        query_builder.build().execute(&pool).await.expect("TODO: panic message");
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
    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<NormalizedRow> {
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

    async fn build_query<T: Sink>(&self, schema: &str, table: &str, columns: &Vec<String>, items: &Vec<Self::RowType>, sink: &T) {
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
