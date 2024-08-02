use async_trait::async_trait;
use mongodb::bson::{Bson, doc, Document};
use mongodb::{Client, Collection, Database};
use crate::metadata::Metadata;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::source::{Sink, Source};
#[derive(Debug)]
pub struct MongoDBSource;

#[derive(Debug)]
pub(crate) enum NormalizedRow {
    I32(i32),
    I64(i64),
    Double32(f32),
    Double64(f64),
    Str(Box<str>),
    String(String),
    None,
}
struct Buffer {
    columns: Option<Vec<String>>,
    buffer: Vec<Vec<NormalizedRow>>,
}
#[async_trait]
impl Source for MongoDBSource {
    type PoolType = (); // Placeholder type, replace it with an actual MongoDB pool type
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

    async fn insert_to<T: Source>(&self, schema: &str, table: &str, columns: &Vec<String>, items: &Vec<Self::RowType>, to: &T) {
        todo!()
    }

    async fn build_query<T: Sink>(&self, schema: &str, table: &str, columns: &Vec<String>, items: &Vec<Self::RowType>, sink: &T) {
        todo!()
    }

    async fn migrate_table_to_cratedb(&self, schema: &str, table: &Collection<Document>, cratedb: CrateDB, metadata: &mut Metadata) {
        println!("Starting migrating table {:?} to CrateDB {:?}", table.name(), cratedb);
        let batch_size: usize = 5000;
        let mut total_documents_sent = 0;
        let mut cursor = table.find(doc! {}).batch_size(batch_size as u32).await.expect("Could not create a cursor");

        let mut buffer: Vec<Vec<NormalizedRow>> = vec![];
        let mut columns: Vec<String> = vec![];

        let mut batch_document_len: Option<usize> = None;
        let ignored_columns = [""];


        metadata.print_step("Starting connection to MongoDB");

        while cursor.advance().await.expect("Could not advance cursor; maybe connection was lost") {
            let mut document = cursor.deserialize_current().unwrap();

            for column in ignored_columns {
                document.remove(column);
            }

            match batch_document_len {
                Some(_) => (),
                None => batch_document_len = Some(document.len())
            }

            // // column is empty on new batches.
            if columns.is_empty() {
               columns  = document.keys().map(|x| String::from(x)).collect();
            }


            let has_doc_len_changed = &batch_document_len.unwrap() != &document.len().clone();
            let normalized = self.row_to_normalized_row(document).await;

            if has_doc_len_changed || &buffer.len() == &batch_size {
                let documents_in_batch: usize = buffer.len();
                cratedb.send_batch(&schema, &table.name(), buffer).await;

                total_documents_sent += &documents_in_batch;
                metadata.print_step(format!("Sent batch of {:?}", &documents_in_batch).as_str());

                // Clear up all temporal containers for the next iteration.
                buffer = vec![];
                batch_document_len = None;


                columns.clear();
            }
            buffer.push(normalized);
        }

        // There is still a last batch.
        // Documents of the last batch is guaranteed to be of the same length.
        if !buffer.is_empty() {
            println!("Second send, {:?}", &buffer.len());
            // cratedb.send_batch(&schema, &table.name(), buffer).await
        }
        // if !buffer.is_empty() {
        //     let first_doc: Document = buffer.first().unwrap().clone();
        //     columns = first_doc.keys().map(|x| String::from(x)).collect();
        //
        //     let mut query_builder = cratedb.get_query_builder_insert(schema, table.name(), &columns).await;
        //     query_builder.push_values(&buffer, |mut b, document: &Document| {
        //         for (_, doc) in document {
        //             match doc {
        //                 Bson::Double(f) => b.push_bind(f),
        //                 Bson::Int32(int) => b.push_bind(int),
        //                 Bson::Int64(int) => b.push_bind(int),
        //                 Bson::String(s) => b.push_bind(s),
        //                 Bson::Boolean(s) => b.push_bind(s),
        //                 Bson::DateTime(d) => b.push_bind(d.try_to_rfc3339_string().unwrap()),
        //                 Bson::Array(a) => {
        //                     match a.first().unwrap() {
        //                         Bson::Int32(u) => {
        //                             let x: Vec<i32> = bson::from_bson(doc.clone()).expect("TODO: panic message - i32 vector");
        //                             b.push_bind(x);
        //                         }
        //                         Bson::Double(_) => {
        //                             let x: Vec<f32> = bson::from_bson(doc.clone()).expect("TODO: panic message - double vector");
        //                             b.push_bind(x);
        //                         }
        //                         _ => {
        //                             let x: Vec<String> /* Type */ = bson::from_bson(doc.clone()).expect("TODO: panic message - string vector");
        //                             b.push_bind(x);
        //                         }
        //                     }
        //                     continue;
        //                 }
        //                 _ => b.push_bind(doc.to_string()),
        //             };
        //         }
        //     });
        //
        //     query_builder.build().execute(&pool).await.expect("Could not send data to CrateDB on last batch");
        //     metadata.print_step(format!("Sent batch of {:?}", &buffer.len()).as_str());
        // }

        metadata.print_step(format!("Total records sent: {}", total_documents_sent).as_str());
        metadata.print_step(format!("Rows per seconds: {}", (total_documents_sent as u128) / metadata.elapsed().as_millis() * 1000).as_str());
    }

    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<NormalizedRow> {
        let mut rows: Vec<NormalizedRow> = Vec::new();
        for (_, doc) in row {
            match doc {
                Bson::Double(v) => rows.push(NormalizedRow::Double64(v)),
                Bson::String(v) => rows.push(NormalizedRow::String(v)),
                Bson::Int32(v) => rows.push(NormalizedRow::I32(v)),
                Bson::Int64(v) => rows.push(NormalizedRow::I64(v)),
                Bson::DateTime(v) => rows.push(NormalizedRow::String(v.try_to_rfc3339_string().unwrap())),
                _ => rows.push(NormalizedRow::String(doc.to_string()))
            }
        }
        return rows;
    }
}
