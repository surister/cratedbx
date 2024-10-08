use std::collections::{HashMap};
use std::str::FromStr;
use mongodb::bson::{doc, Bson, Document};
use mongodb::Collection;
use crate::experiment::data::{get_inner_cvalue_type_name, CColumn, CDataFrame, CValue, CValueType, DtypeStrategy};
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
        let expected_dtype: Option<CValueType> = get_expected_dtype(&schema, column);
        let strategy: Option<DtypeStrategy> = get_strategy(&schema, column);

        if let (Some(expected_dtype), Some(strategy)) = (expected_dtype, strategy) {
            row.data_type = expected_dtype.clone();
            row.expected_dtype = expected_dtype.clone();
            for (i, value) in row.values.iter_mut().enumerate() {

                // If the expected dtype is not equal to the value dtype
                if !&value.equals_dtype(&CValue::None) && !value.is_dtype(expected_dtype) {
                    match strategy {
                        DtypeStrategy::NewCol => {
                            new_rows.push((value.clone(), i, column.clone()));
                            *value = CValue::None;
                        }

                        DtypeStrategy::Cast => {
                            match (&expected_dtype, &value) {
                                (CValueType::String, CValue::String(_)) => continue,
                                (CValueType::String, CValue::VecString(v)) => *value = CValue::String(v.join(" ")),
                                (CValueType::I32, CValue::String(v)) => *value = CValue::I32(v.parse().unwrap()),
                                (CValueType::String, CValue::I32(v)) => *value = CValue::String(v.to_string()),
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
            let expected_dtype = get_expected_dtype(&schema, &new_column_name).or(Some(CValueType::Unknown));
            let dtype = value.get_dtype();
            last_padding = i;
            col_values.extend(vec![CValue::None; i]);
            col_values.push(value);
            dataframe.add_column(
                new_column_name,
                CColumn {
                    values: col_values,
                    expected_dtype: expected_dtype.unwrap(),
                    data_type: dtype,
                    dtype_strategy: DtypeStrategy::Ignore
                });
        } else {
            col_values.extend(vec![CValue::None; i - last_padding]);
            col_values.push(value);

            dataframe.add_values_to_column(new_column_name, col_values);
        }
    }



    dataframe
}

fn get_expected_dtype(schema: &CSchema, column_name: &str) -> Option<CValueType> {
    schema
        .columns
        .get(column_name)
        .and_then(|column_info| Option::from(column_info.dtype))
}

fn get_strategy(schema: &CSchema, column_name: &str) -> Option<DtypeStrategy> {
    schema.columns.get(column_name).and_then(|column_info| Option::from(column_info.dtype_collision_strategy.clone()))
}

pub async fn iter_cols(table: &Collection<Document>, metadata: &mut Metadata) {
    let data = r#"
        {
            "name": {"dtype": "String", "dtype_collision_strategy": "NewCol", "sub_schema": { "sub_id": {"dtype": "I32", "dtype_collision_strategy": "NewCol"} }},
            "another_col": {"dtype": "String", "dtype_collision_strategy": "NewCol"},
            "sub_id": {"dtype": "I32", "dtype_collision_strategy": "NewCol"},
            "k": {"dtype": "Object", "dtype_collision_strategy": "Cast"}
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
    dataframe = dataframe.select(vec!["k".to_string()]);
    dataframe.print_schema();
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