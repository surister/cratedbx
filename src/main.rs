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
    let mongodb = MongoDBSource {};
    let postgres = PostgresSource {};
    let table = mongodb.get_table("sample_mflix", "comments").await.unwrap();

    // mongodb.migrate_table_to_cratedb("doc", &table, cratedb, &mut metadata).await;

    postgres.migrate_table_to_cratedb("public", &String::from("comments"), cratedb, &mut metadata).await;
    metadata.print_total_duration();
}