use async_trait::async_trait;

mod utils;
mod source;
mod sink;
mod metadata;

use source::mongodb::driver::MongoDBSource;
use crate::sink::cratedb::driver::CrateDB;

use crate::metadata::Metadata;
use crate::source::postgres::driver::PostgresSource;
use crate::source::source::Source;



#[tokio::main]
async fn main() {
    let mut metadata = Metadata::new();
    metadata.start();

    let cratedb = CrateDB {};
    let mongodb = MongoDBSource {
        uri: String::from("mongodb+srv://admin:admin@cluster0.lpm7jyz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    };
    let postgres = PostgresSource {};
    let table = mongodb.get_table("test_db", "simple_array").await.unwrap();
    let ignored_columns: Vec<&str> = vec!["simple_array"];
    mongodb.migrate_table_to_cratedb("doc", &table, ignored_columns, cratedb, &mut metadata).await;

    // postgres.migrate_table_to_cratedb("public", &String::from("comments"), cratedb, &mut metadata).await;
    metadata.print_total_duration();
}