mod utils;
mod source;
mod sink;
mod metadata;

use source::mongodb::driver::MongoDBSource;
use crate::sink::cratedb::driver::CrateDB;
use crate::source::source::Source;
use crate::metadata::Metadata;

#[tokio::main]
async fn main() {
    let mut metadata = Metadata::new();
    metadata.start();

    let cratedb = CrateDB {};
    let mongodb = MongoDBSource {};

    let table = mongodb.get_table("sample_mflix", "comments").await.unwrap();

    mongodb.migrate_table_to_cratedb("public", &table, cratedb, &mut metadata).await;

    metadata.print_total_duration()
}