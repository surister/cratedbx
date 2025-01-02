mod utils;
mod source;
mod sink;
mod metadata;
mod experiment;

use std::any::Any;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use arrow::array::{Array, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray, UInt32Array, UInt64Array};
use arrow::compute::kernels::numeric::rem;
use arrow::datatypes::{DataType, Int64Type};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
use parquet::arrow::ProjectionMask;
use tokio::runtime::Handle;
use source::mongodb::driver::MongoDBSource;
use crate::experiment::data::{CDataFrame, CValue};
use crate::sink::cratedb::driver::CrateDB;

use crate::metadata::Metadata;
use crate::source::mysql::driver::MySqlSource;
use crate::source::postgres::driver::PostgresSource;
use crate::source::source::{Sink, Source};

async fn t1() {
    println!()
}

async fn t2(){
    t1().await;
    println!("t2")
}

fn main() {
    let mut metadata = Metadata::new();
    metadata.start();

    let cratedb = CrateDB {};

    let mongodb = MongoDBSource {
        uri: String::from("mongodb://localhost")
    };

    let postgres = PostgresSource {
        uri: "postgres://postgres:postgres@192.168.88.251:5400/postgres".to_string()
    };

    let mysql = MySqlSource {
        uri: "mysql://root:mysql@localhost:3306".to_string(),
    };
    //unstructured_array
    // let table = mongodb.get_table("testdb", "unstructured_array").await.unwrap();

    let ignored_columns: Vec<&str> = vec![
        "_id",
        // "id",
        "txt",
        "ip",
        "i16",
        "u16",
        "i32",
        "u32",
        "i32_from_text",
        "f32",
        "f64",
        "bool",
        "null_",
        "datetime",
        "datetime_2",
        "obj",
        "vector_float_simple",
        "vector_float",
        "array_i32",
        "array_u32",
        "array_text",
        "empty_array",
        "array_mixed",
        "empty_multi_arrays",
        "empty_nested_array",
        "empty_nested_array_2",
        "empty_nested_array_3",
        "empty_nested_array_4",
        "empty_nested_array_5",
    ];

    use arrow::record_batch::RecordBatchReader;
    use std::fs::File;

    let file = File::open("/home/surister/RustroverProjects/cdctest/data_id.parquet").unwrap();
    let file2 = File::open("/home/surister/RustroverProjects/cdctest/data_id.parquet").unwrap();
    let b = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap().metadata().file_metadata().num_rows();
    println!("{:?}", b);
    let total = b;
    let cpu_cores = 24;
    let batch_size = 15000;

    let rows_per_group = total / cpu_cores;
    let remainder = total % cpu_cores;
    let mut handles = vec![];
    for i in 0..cpu_cores {
        let low_end = rows_per_group * (i);
        let mut high_end = rows_per_group;

        if i == (cpu_cores - 1) {
            high_end += remainder;
        }

        let h = thread::spawn(move || {
            // barrier.wait();
            let thread_id = thread::current().id();
            let file = File::open("/home/surister/RustroverProjects/cdctest/data.parquet").unwrap();
            let selectors = vec![
                RowSelector::skip(low_end as usize),
                RowSelector::select(high_end as usize),
            ];
            println!("{:?}", selectors);
            // Creating a selection will combine adjacent selectors
            let selection: RowSelection = selectors.into();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap().with_row_selection(selection).with_batch_size(batch_size);

            let parquet_schema = builder.parquet_schema();
            let projection = ProjectionMask::roots(parquet_schema, vec![0]);
            let mut reader = builder
                .build()
                .unwrap();
            println!("thread_id, {:?}", thread_id);
            while let Some(rows) = reader.next() {
                let r = rows.unwrap();
                let columns: Vec<String> = r
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone()) // Clone the column names
                    .collect();
                println!("record batch gotten {:?}, from {:?}", r.num_rows(), thread_id);
                let records = record_batch_to_cvalues(r);
                let rows = cvalues_column_to_rows(records);

                cratedb.send_batch_http_sync("doc", "taxi", &columns, rows);
            }
        });
        handles.push(h);


    }
    for h_ in handles {
        h_.join().unwrap();
    }


    // let columns: Vec<String> = reader.schema().fields().iter().map(|x| x.name().to_string()).collect();
    fn record_batch_to_cvalues(record_batch: RecordBatch) -> Vec<Vec<CValue>> {
        let mut result = Vec::new();

        for column in record_batch.columns() {
            let cast_column = match column.data_type() {
                // DataType::Boolean => {
                //     let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                //     CValue::VecDyn(array.iter().map(|x| CValue::Bool(x.unwrap_or(false))).collect())
                // }
                // DataType::Int16 => {
                //     let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                //     CValue::VecI32(array.iter().map(|x| x.unwrap_or(0) as i32).collect())
                // }
                DataType::UInt32 => {
                    let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                    let vals: Vec<CValue> = array.into_iter().map(|v| CValue::U32(v.unwrap_or(0))).collect();
                    vals
                }
                DataType::Int32 => {
                    let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                    let vals: Vec<CValue> = array.into_iter().map(|v| CValue::I32(v.unwrap_or(0))).collect();
                    vals
                }
                DataType::Int64 => {
                    let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    let vals: Vec<CValue> = array.into_iter().map(|v| CValue::I64(v.unwrap_or(0))).collect();
                    vals
                }
                DataType::Float32 => {
                    let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                    let vals: Vec<CValue> = array.into_iter().map(|v| CValue::Double32(v.unwrap_or(0.0))).collect();
                    vals
                }
                DataType::Float64 => {
                    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    let vals: Vec<CValue> = array.into_iter().map(|v| CValue::Double64(v.unwrap_or(0.0))).collect();
                    vals
                }
                DataType::Utf8 => {
                    let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                    let vals: Vec<CValue> = array.into_iter().map(|v| CValue::String(v.unwrap_or("").to_string())).collect();
                    vals
                }
                DataType::LargeUtf8 => {
                    let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    let vals: Vec<CValue> = array.into_iter().map(|v| CValue::String(v.unwrap_or("").to_string())).collect();
                    vals
                }
                DataType::Timestamp(u, _) => {
                    let array = column.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    let vals: Vec<CValue> = array.into_iter().map(|v| CValue::I64(v.unwrap_or(0))).collect();
                    vals
                }
                _ => {
                    println!("{:?} MISSING", column.data_type());
                    vec![CValue::Unknown]
                }
            };
            result.push(cast_column);
        }

        result
    }
    // let mut columns: Vec<&String> = vec![];
    // let columns: Vec<String> = vec!["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee"].iter().map(|s| s.to_string()).collect();;
    fn cvalues_column_to_rows(columns: Vec<Vec<CValue>>) -> Vec<Vec<CValue>> {
        let mut rows: Vec<Vec<CValue>> = vec![];
        for i in 0..columns[0].len() {
            let mut row = vec![];
            for col in 0..columns.len() {
                row.push(columns[col][i].clone());
            }
            rows.push(row);
        }
        rows
    }


    // let x = rows.into_iter().map(|record_batch| record_batch_to_cvalue(record_batch));


    // iter_cols(&table, &mut metadata).await;


    // mongodb.migrate_table_to_cratedb("doc", &table, ignored_columns, cratedb, &mut metadata).await;
    // mongodb.migrate_table_to_cratedb_pg("doc", &table, ignored_columns, cratedb, &mut metadata).await;
    // postgres.migrate_table_to_cratedb("public", &String::from("simple_array"), ignored_columns, cratedb, &mut metadata).await;
    // mysql.migrate_table_to_cratedb("mysql", &"simple_array".to_string(), ignored_columns, cratedb, &mut metadata).await;
    metadata.print_total_duration();
}