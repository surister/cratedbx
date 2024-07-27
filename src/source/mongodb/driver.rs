use async_trait::async_trait;
use mongodb::bson::{Bson, doc, Document};
use mongodb::{bson, Client, Collection, Database};
use sqlx::{Execute, Postgres, QueryBuilder};
use crate::sink::cratedb::driver::CrateDB;
use crate::source::source::Source;
#[derive(Debug)]
pub struct MongoDBSource;

#[async_trait]
impl Source for MongoDBSource {
    type PoolType = (); // Placeholder type, replace with actual MongoDB pool type
    type ErrorType = ();
    type ClientType = Client;
    type DatabaseType = Database;
    type TableType = Collection<Document>;
    type RowType = Document;

    async fn get_pool(&self) -> Result<Self::PoolType, Self::ErrorType> {
        todo!() // MongoDB handles pooling internally.
    }
    async fn get_client(&self) -> Result<Self::ClientType, Self::ErrorType> {
        let client_uri = "";

        let client = Client::with_uri_str(&client_uri).await;
        return Ok(client.unwrap());
    }
    async fn list_databases(&self) -> Result<Vec<String>, Self::ErrorType> {
        let client = self.get_client().await;
        let databases = client.unwrap().list_database_names().await;
        return Ok(databases.unwrap());
    }
    async fn list_tables(&self, database: &str) -> Result<Vec<String>, Self::ErrorType> {
        let client = self.get_client().await;
        let collections = client.unwrap().database(&database).list_collection_names().await;
        return Ok(collections.unwrap());
    }
    async fn get_database(&self, database: &str) -> Result<Self::DatabaseType, Self::ErrorType> {
        let client = self.get_client().await;
        let database = client.unwrap().database(&database);
        return Ok(database);
    }
    async fn get_table(&self, database: &str, table_name: &str) -> Result<Self::TableType, Self::ErrorType> {
        Ok(self.get_database(&database).await.unwrap().collection(&table_name))
    }
    async fn migrate_table<T: Source>(&self, to: &T) {
        println!("");
    }

    async fn insert_to<T: Source>(&self, schema: &str, table: &str, items: &Vec<Self::RowType>, to: &T) {

    }

    async fn migrate_table_to_cratedb(&self, table: &Collection<Document>, cratedb: CrateDB) {
        println!("Starting migrating table {:?} to CrateDB {:?}", table.name(), cratedb);
        let batch_size: usize = 5000;
        let mut cursor = table.find(doc! {}).batch_size(batch_size as u32).await.expect("Could not create a cursor");

        let mut buffer: Vec<Document> = vec![];
        let mut columns: Vec<String> = vec![];

        let mut batch_document_len: Option<usize> = None;
        let ignored_columns = ["_id"];

        while cursor.advance().await.expect("Could not advance cursor; maybe connection was lost") {
            let mut document = cursor.deserialize_current().unwrap();

            for column in ignored_columns {
                document.remove(column);
            }

            match batch_document_len {
                Some(_) => (),
                None => batch_document_len = Some(document.len())
            }

            // column is empty on new batches.
            if columns.is_empty() {
                columns = document.keys().map(|x| String::from(x)).collect();
            }
            self.insert_to("doc", &table.name(), &buffer, &cratedb).await;
            let mut query_builder: QueryBuilder<Postgres> = cratedb.get_query_builder_insert("doc", &table.name().to_string(), columns);

            columns = vec![];
        }
    }
}
