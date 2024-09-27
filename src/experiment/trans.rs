use std::collections::{HashMap};
use std::str::FromStr;
use mongodb::bson::{doc, Bson, Document};
use mongodb::Collection;
use crate::experiment::data::{get_inner_cvalue_type_name, CColumn, CDataFrame, CValue, DtypeStrategy};
use crate::experiment::schema::CSchema;
use crate::metadata::Metadata;


fn row_to_normalized_row(row: Document) -> Vec<CValue> {
    // Transform a mongodb Document into a vector of CValue(s)
    row.into_iter().map(|(_, v)| { bson_to_cvalue(v) }).collect()
}

pub fn bson_to_cvalue(row: Bson) -> CValue {
    match row {
        Bson::String(v) => CValue::String(v),
        Bson::Int32(v) => CValue::I32(v),
        Bson::Int64(v) => CValue::I64(v),
        Bson::Double(v) => CValue::Double64(v),
        Bson::Decimal128(v) => CValue::Double64(v.to_string().parse().unwrap()),
        Bson::ObjectId(v) => CValue::String(v.to_string()),
        Bson::Boolean(v) => CValue::Bool(v),
        Bson::Null => CValue::None,
        Bson::Array(v) => {
            if v.is_empty() {
                return CValue::VecString(vec![])
            }
            let vals: Vec<CValue> = v.into_iter().map(|el| bson_to_cvalue(el)).collect();
            CValue::VecDyn(vals)
        }
        Bson::DateTime(v) => CValue::I64(v.timestamp_millis()),
        Bson::Document(v) => {
            let mut map: HashMap<String, CValue> = HashMap::new();
            for (k, v) in v {
                map.insert(k, bson_to_cvalue(v));
            }
            CValue::Object(map)
        }

        _ => {
            CValue::String(row.to_string())
        }

    }
}



fn check_dataset(mut dataframe: CDataFrame, schema: CSchema) -> CDataFrame {
    let mut new_rows: Vec<(CValue, usize, String)> = vec![];

    // We go through every defined column in the Schema and resolve its schema, if there is a value dtype mismatch
    // the DtypeStrategy solution will be applied to resolve it.
    for (column, row) in &mut dataframe.columns {
        let expected_dtype: Option<CValue> = get_expected_dtype(&schema, column);
        let strategy: Option<DtypeStrategy> = get_strategy(&schema, column);

        if let (Some(expected_dtype), Some(strategy)) = (expected_dtype, strategy) {
            row.data_type = expected_dtype.clone();
            row.expected_dtype = expected_dtype.clone();
            for (i, value) in row.values.iter_mut().enumerate() {

                // If the expected dtype is not equal to the value dtype
                if !value.equals_dtype(&CValue::None) & !expected_dtype.equals_dtype(value) {
                    match strategy {
                        DtypeStrategy::new_col => {
                            new_rows.push((value.clone(), i, column.clone()));
                            *value = CValue::None;
                        }

                        DtypeStrategy::cast => {
                            match (&expected_dtype, &value) {
                                (CValue::String(_), CValue::String(_)) => continue,
                                (CValue::String(_), CValue::VecString(v)) => *value = CValue::String(v.join(" ")),
                                (CValue::I32(_), CValue::String(v)) => *value = CValue::I32(v.parse().unwrap()),
                                (CValue::String(_), CValue::I32(v)) => *value = CValue::String(v.to_string()),
                                _ => continue
                            }
                        }
                        _ => continue
                    }
                }
            }
        }
    }

    let mut last_padding: usize = 0;

    // Add new rows for strategy new_column.
    for (value, i, column) in new_rows {
        let mut col_values: Vec<CValue> = Vec::with_capacity(i + 1);
        let new_column_name = format!("{}_{}", column, get_inner_cvalue_type_name(&value)).to_string();

        if !dataframe.has_column(&new_column_name) {
            let expected_dtype = get_expected_dtype(&schema, &new_column_name).or(Some(CValue::Unknown));
            let dtype = value.clone();
            last_padding = i;
            col_values.extend(vec![CValue::None; i]);
            col_values.push(value);
            dataframe.add_column(new_column_name, CColumn { values: col_values, expected_dtype: expected_dtype.unwrap(), data_type: dtype });
        } else {
            col_values.extend(vec![CValue::None; i - last_padding]);
            col_values.push(value);

            dataframe.add_values_to_column(new_column_name, col_values);
        }
    }

    for (column_name, row) in &mut dataframe.columns {
        if row.data_type.equals_dtype(&CValue::VecDyn(vec![])) {
            // println!("{:?}", row);
        }
    }

    dataframe
}

fn get_expected_dtype(schema: &CSchema, column_name: &str) -> Option<CValue> {
    schema
        .columns
        .get(column_name)
        .and_then(|column_info| column_info.dtype.as_str().parse().ok())
}

fn get_strategy(schema: &CSchema, column_name: &str) -> Option<DtypeStrategy> {
    schema.columns.get(column_name).and_then(|column_info| column_info.dtype_collision_strategy.as_str().parse().ok())
}

pub async fn iter_cols(table: &Collection<Document>, metadata: &mut Metadata) {
    let data = r#"
        {
            "name": {"dtype": "string", "dtype_collision_strategy": "new_col", "sub_schema": { "sub_id": {"dtype": "i32", "dtype_collision_strategy": "new_col"} }},
            "another_col": {"dtype": "String", "dtype_collision_strategy": "new_col"},
            "sub_id": {"dtype": "i32", "dtype_collision_strategy": "new_col"},
            "k": {"dtype": "object", "dtype_collision_strategy": "cast"}
        }"#;

    let schema: CSchema = serde_json::from_str(data).unwrap();
    let batch_size: usize = 3000;
    let mut buffer = vec![];
    let mut cursor = table.find(doc! {}).batch_size(batch_size as u32).await.expect("Could not create a cursor in MongoDB, is the server up?");


    while cursor.advance().await.expect("Could not advance cursor; maybe Mongo connection was lost") {
        let document = cursor.deserialize_current().unwrap();
        buffer.push(document);
    }

    let mut dataframe = CDataFrame::from_bson(buffer, schema);

    // dataframe = dataframe.select(vec![
    //     "id".to_string(),
    //     "_id".to_string(),
    //     "k".to_string()
    // ]);
    dataframe.print();

    // print_dataset(&dataframe,
    //               Some(vec![
    //                   "k".to_string(),
    //                   "id".to_string(),
    //                   "name_i32".to_string(),
    //                   "id".to_string(),
    //                   "name".to_string(),
    //                   "sub_id".to_string(),
    //                   "surname".to_string()
    //               ]));
    // let new_dataset = check_dataset(dataframe, expected_schema);

    // print_dataset(&new_dataset, Some(vec![
    //     "k".to_string(),
    //     "_id".to_string(),
    //     "another".to_string(),
    //     "id".to_string(),
    //     "name_i32".to_string(),
    //     "id".to_string(),
    //     "name".to_string(),
    //     "sub_id".to_string(),
    //     "surname".to_string(),
    //     "sub_id_str".to_string(),
    //     "name_vecdyn".to_string(),
    // ]));
}