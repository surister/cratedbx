use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use async_trait::async_trait;

use mongodb::bson::{Bson, doc, Document};
use mongodb::{Client, Collection, Database};

use serde::{Deserialize, Serialize};
use crate::experiment::data::CValue;
use crate::metadata::Metadata;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::source::{Sink, Source};

#[derive(Debug)]
pub struct MongoDBSource {
    pub(crate) uri: String,
}


// Transform this into its own function: `is_same_dtype(other) -> bool`




#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum StringRow {
    String(String),
    I32(i32),
    U32(u32),
    I64(i64),
    Vec(Vec<String>),
    VecI32(Vec<i32>),
    VecF64(Vec<f64>),
    None,
}


fn bson_to_normalized_row(row: Bson) -> CValue {
    match row {
        Bson::Double(v) => CValue::Double64(v),
        Bson::String(v) => CValue::String(v),
        Bson::Int32(v) => CValue::I32(v),
        Bson::Int64(v) => CValue::I64(v),
        Bson::Decimal128(v) => CValue::Double64(v.to_string().parse().unwrap()),
        Bson::ObjectId(v) => CValue::String(v.to_string()),
        Bson::Null => CValue::None,
        Bson::Array(v) => {
            // Right now we do not support arrays inside arrays.
            // [ [1,2,3], [4,5,6] ]
            // This translates to Array [String("[...]"), String("[...]")], to support it
            // we should iterate further (match if content is an array) and extract those, alternatively
            // we could probably just wrap it in an object; e.g. { obj: [[1,], [2,]]}
            match &v.first() {
                // Array is empty
                None => CValue::VecString(vec![]),

                // Array has contents
                Some(first_element) => {
                    match first_element {
                        Bson::Double(_) => CValue::VecF64(v.into_iter().map(|x| x.as_f64().expect(format!("Could not unwrap value of type '{:?}' to f64, is your array of mixed types?", x.element_type()).as_str())).collect::<Vec<f64>>()),
                        Bson::Int32(_) => CValue::VecI32(v.into_iter().map(|x| x.as_i32().expect(format!("Could not unwrap value of type '{:?}' to i32, is your array of mixed types?", x.element_type()).as_str())).collect::<Vec<i32>>()),
                        Bson::Int64(_) => CValue::VecI64(v.into_iter().map(|x| x.as_i64().expect(format!("Could not unwrap value of type '{:?}' to i64, is your array of mixed types?", x.element_type()).as_str())).collect::<Vec<i64>>()),
                        _ => CValue::VecString(v.into_iter().map(|x| {
                            match x {
                                Bson::String(s) => s,
                                _ => x.to_string()
                            }
                        }).collect::<Vec<String>>())
                    }
                }
            }
        }
        Bson::DateTime(v) => CValue::I64(v.timestamp_millis()),
        // Bson::Array(v) => v.into_iter().map(|x| bson_to_normalized_row(x)).collect(),
        _ => {
            CValue::String(row.to_string())
        }
    }
}


fn bson_to_string(row: Bson) -> StringRow {
    match row {
        Bson::ObjectId(v) => StringRow::String(v.to_string()),
        Bson::Array(v) => {
            // Right now we do not support arrays inside arrays.
            // [ [1,2,3], [4,5,6] ]
            // This translates to Array [String("[...]"), String("[...]")], to support it
            // we should iterate further (match if content is an array) and extract those, alternatively
            // we could probably just wrap it in an object; e.g. { obj: [[1,], [2,]]}
            match &v.first() {
                // Array is empty
                None => StringRow::Vec(vec![]),

                // Array has contents
                Some(first_element) => {
                    match first_element {
                        Bson::Double(_) => StringRow::VecF64(v.into_iter().map(|x| x.as_f64().unwrap()).collect::<Vec<f64>>()),
                        _ => StringRow::Vec(v.into_iter().map(|x| {
                            match x {
                                Bson::String(s) => s,
                                _ => x.to_string()
                            }
                        }).collect::<Vec<String>>())
                    }
                }
            }
        }
        Bson::String(v) => StringRow::String(v),
        Bson::Int32(v) => StringRow::I32(v),
        Bson::Int64(v) => StringRow::I64(v),
        Bson::DateTime(v) => StringRow::String(v.try_to_rfc3339_string().unwrap()),
        Bson::Null => StringRow::None,
        _ => StringRow::String(row.to_string())
    }
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
        let client = Client::with_uri_str(&self.uri).await;
        Ok(client.unwrap())
    }
    async fn list_databases(&self) -> Result<Vec<String>, Self::ErrorType> {
        let client = self.get_client().await;
        let databases = client.unwrap().list_database_names().await;
        Ok(databases.unwrap())
    }
    async fn list_tables(&self, database: &str) -> Result<Vec<String>, Self::ErrorType> {
        let client = self.get_client().await;
        let collections = client.unwrap().database(&database).list_collection_names().await;
        Ok(collections.unwrap())
    }
    async fn get_database(&self, database: &str) -> Result<Self::DatabaseType, Self::ErrorType> {
        let client = self.get_client().await;
        let database = client?.database(&database);
        Ok(database)
    }
    async fn get_table(&self, database: &str, table_name: &str) -> Result<Self::TableType, Self::ErrorType> {
        Ok(self.get_database(&database).await?.collection(&table_name))
    }

    async fn count(&self, database: &str, table_name: &str) -> Result<i64, Self::ErrorType> {
        todo!()
    }

    async fn migrate_table_to_cratedb_pg(&self, schema: &str, table: &Collection<Document>, ignored_columns: Vec<&str>, cratedb: CrateDB, metadata: &mut Metadata) {
        println!("Starting migrating table {:?} to CrateDB {:?}", table.name(), cratedb);
        let mut total_documents_sent = 0;
        let batch_size: usize = 3000;
        let mut cursor = table.find(doc! {}).batch_size(batch_size as u32).await.expect("Could not create a cursor");

        let mut buffer: Vec<Vec<CValue>> = vec![];
        let mut columns: Vec<String> = vec![];
        let mut last_columns: Vec<String> = vec![];

        let mut batch_document_len: Option<usize> = None;

        metadata.print_step("Starting connection to MongoDB");

        while cursor.advance().await.expect("Could not advance cursor; maybe connection was lost") {
            let mut document = cursor.deserialize_current().unwrap();

            for column in &ignored_columns {
                document.remove(column);
            }

            match batch_document_len {
                Some(_) => (),
                None => batch_document_len = Some(document.len())
            }

            let has_doc_len_changed = &batch_document_len.unwrap() != &document.len().clone();

            // column is empty on new batches.
            if columns.is_empty() {
                columns = document.keys().map(|x| String::from(x)).collect();
            }

            if has_doc_len_changed || last_columns.is_empty() {
                // This fixes getting the columns for the last batch.
                last_columns = document.keys().map(|x| String::from(x)).collect();
            }

            let normalized_row = self.row_to_normalized_row(document).await;

            if has_doc_len_changed || &buffer.len() == &batch_size {
                let documents_in_batch: usize = buffer.len();
                cratedb.send_batch(&schema, &table.name(), &columns, buffer).await;

                total_documents_sent += &documents_in_batch;
                metadata.print_step(format!("Sent batch of {:?}", &documents_in_batch).as_str());

                // Clear up all temporal containers for the next iteration.
                buffer = vec![];
                batch_document_len = None;
                columns.clear();
            }
            buffer.push(normalized_row);
        }

        // There is still a last batch.
        // Rows of the last batch are guaranteed to be of the same length.
        if !buffer.is_empty() {
            let documents_in_batch: usize = buffer.len();
            total_documents_sent += &documents_in_batch;
            cratedb.send_batch(&schema, &table.name(), &last_columns, buffer).await
        }

        metadata.print_step(format!("Total records sent: {}", total_documents_sent).as_str());
        metadata.print_step(format!("Rows per seconds: {}", (total_documents_sent as u128) / metadata.elapsed().as_millis() * 1000).as_str());
    }

    async fn migrate_table_to_cratedb(&self, schema: &str, table: &Collection<Document>, ignored_columns: Vec<&str>, cratedb: CrateDB, metadata: &mut Metadata) {
        println!("Starting migrating table {:?} to CrateDB {:?}", table.name(), cratedb);
        let batch_size: usize = 1;
        let mut total_documents_sent = 0;
        let mut cursor = table.find(doc! {}).batch_size(batch_size as u32).await.expect("Could not create a cursor");

        let mut buffer: Vec<Vec<StringRow>> = vec![];
        let mut current_batch_columns: Vec<String> = vec![];
        let mut last_columns: Vec<String> = vec![];

        let mut batch_document_len: Option<usize> = None;

        metadata.print_step("Starting connection to MongoDB");
        println!("??Asdf");
        while cursor.advance().await.expect("Could not advance cursor; maybe connection was lost") {
            let mut document = cursor.deserialize_current().unwrap();
            println!("{:?} focking doc mate", document);
            for column in &ignored_columns {
                document.remove(column);
            }

            match batch_document_len {
                Some(_) => (),
                None => batch_document_len = Some(document.len())
            }

            let has_doc_len_changed = &batch_document_len.unwrap() != &document.len().clone();

            // column is empty on new batches.
            if current_batch_columns.is_empty() {
                current_batch_columns = document.keys().map(|x| String::from(x)).collect();
            }

            if has_doc_len_changed || last_columns.is_empty() {
                // This fixes getting the columns for the last batch.
                last_columns = document.keys().map(|x| String::from(x)).collect();
            }

            let normalized_row = self.row_to_vec_str(document);

            if has_doc_len_changed || &buffer.len() == &batch_size {
                let documents_in_batch: usize = buffer.len();
                cratedb.send_batch_http(&schema, &table.name(), &current_batch_columns, buffer).await;

                total_documents_sent += &documents_in_batch;
                metadata.print_step(format!("Sent batch of {:?}", &documents_in_batch).as_str());

                // Clear up all temporal containers for the next iteration.
                buffer = vec![];
                batch_document_len = None;
                current_batch_columns.clear();
            }
            buffer.push(normalized_row);
        }

        // There is still a last batch.
        // Rows of the last batch are guaranteed to be of the same length.
        if !buffer.is_empty() {
            let documents_in_batch: usize = buffer.len();
            total_documents_sent += &documents_in_batch;
            cratedb.send_batch_http(&schema, &table.name(), &last_columns, buffer).await;
            total_documents_sent += &documents_in_batch;
            metadata.print_step(format!("Sent batch of {:?}", &documents_in_batch).as_str());
        }

        metadata.print_step(format!("Total records sent: {}", total_documents_sent).as_str());
        metadata.print_step(format!("Rows per seconds: {}", (total_documents_sent as u128) / metadata.elapsed().as_millis() * 1000).as_str());
    }

    async fn row_to_normalized_row(&self, row: Self::RowType) -> Vec<CValue> {
        let mut rows: Vec<CValue> = Vec::new();
        for (_, value) in row {
            rows.push(bson_to_normalized_row(value))
        }
        return rows;
    }

    fn row_to_vec_str(&self, row: Self::RowType) -> Vec<StringRow> {
        let mut rows: Vec<StringRow> = Vec::new();
        for (_, value) in row {
            rows.push(bson_to_string(value))
        }
        rows
    }
}
