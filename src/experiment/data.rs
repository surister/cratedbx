use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Formatter;
use std::str::FromStr;
use indexmap::IndexMap;
use indexmap::map::Entry;
use mongodb::bson::{Document};
use serde::{Deserialize, Serialize};
use crate::experiment::schema::CSchema;
use crate::experiment::trans::bson_to_cvalue;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DtypeStrategy {
    NewCol,
    Cast,
    Ignore,
    Remove
}

pub struct ParseDtypeStrategyError {
    message: String,
}

impl FromStr for DtypeStrategy {
    type Err = ParseDtypeStrategyError;


    fn from_str(input: &str) -> Result<DtypeStrategy, ParseDtypeStrategyError> {
        match input.to_lowercase().as_str() {
            "new_col" => Ok(DtypeStrategy::NewCol),
            "cast" => Ok(DtypeStrategy::Cast),
            "ignore" => Ok(DtypeStrategy::Ignore),
            "remove" => Ok(DtypeStrategy::Remove),
            _ => Err(ParseDtypeStrategyError { message: format!("'{}' is not a valid CValue", input) }),
        }
    }
}


#[derive(Debug)]
pub struct CDataFrame {
    pub columns: IndexMap<String, CColumn>,
    pub count: usize,
    selected_columns: Vec<String>,
}

impl CDataFrame {
    /// Instantiate an empty CDataFrame.
    pub fn new() -> Self {
        Self {
            columns: IndexMap::new(),
            count: 0,
            selected_columns: vec![],
        }
    }

    pub fn from_bson(documents: Vec<Document>, schema: CSchema) -> Self {
        let mut new_dataframe = Self::new();

        // Track the current columns in the dataset.
        let mut dataset_columns: HashSet<String> = HashSet::new();
        let mut row_count: usize = 0;

        // Track the columns of the current document.
        for document in documents {
            let doc_columns: Vec<String> = document.keys().map(|x| String::from(x)).collect();

            for col in &doc_columns {
                if !new_dataframe.has_column(col) {

                    // If a new column is found, add it to the dataset.
                    dataset_columns.insert(col.to_string());

                    // Fill the columns with nulls, so if we insert a new column at length 8, 1to7 are not empty, but have nulls.
                    new_dataframe.add_column(
                        col.to_string(),
                        CColumn {
                            values: vec![CValue::None; row_count],
                            data_type: CValueType::Unknown,
                            expected_dtype: CValueType::Unknown,
                            dtype_strategy: DtypeStrategy::Ignore,
                        });
                }
            }

            for (col, value) in document {
                new_dataframe.add_value_to_column(&col, bson_to_cvalue(value))
            }

            for col in &dataset_columns {
                if !doc_columns.contains(&col) {
                    // If a document does not contain a key in the dataframe, add a None, so all
                    // columns have the same length.
                    new_dataframe.add_value_to_column(col, CValue::None)
                }
            }
            row_count += 1;
        }
        new_dataframe.count = row_count;
        new_dataframe.set_schema(schema);
        new_dataframe
    }

    pub fn select(mut self, columns: Vec<String>) -> Self {
        if !self.selected_columns.is_empty() {
            self.selected_columns.clear()
        }

        for col in columns {
            if self.has_column(&col) {
                self.selected_columns.push(col)
            }
        }
        self
    }

    pub fn print_schema(&self) {
        for (name, column) in &self.columns {
            println!("{:?} expected_dtype: {:?}, dtype: {:?}, strategy: {:?}",
                     name,
                     column.expected_dtype,
                     column.data_type,
                     column.dtype_strategy
            )
        }
    }

    pub fn print(&self) {
        let headers = if self.selected_columns.is_empty() {
            self.columns.iter().map(|(x, c)| x.to_string()).collect()
        } else { self.selected_columns.clone() };
        // println!("{:?}", headers);
        for key in &headers {
            print!("| {:^10}", key, );
        }
        println!(); // Move to the next line.

        for i in 0..self.count {
            for key in &headers {
                let vec = &self.columns[key].values;
                if i < vec.len() {
                    print!("| {:^1} ", vec[i]);
                } else {
                    print!("{:<10} ", " ");
                }
            }
            println!(); // Move to the next line after each row
        }
    }

    fn set_schema(&mut self, schema: CSchema) {
        for (name, column) in schema.columns {
            if self.has_column(&name) {
                self.modify_column(name, |x| {
                    x.expected_dtype = column.dtype;
                    x.dtype_strategy = column.dtype_collision_strategy;
                })
            }
        }
    }

    pub fn from_bson(documents: Vec<Document>, schema: CSchema) -> Self {
        let mut new_dataframe = Self::new();

        // Track the current columns in the dataset.
        let mut dataset_columns: HashSet<String> = HashSet::new();
        let mut row_count: usize = 0;

        // Track the columns of the current document.
        for document in documents {
            let doc_columns: Vec<String> = document.keys().map(|x| String::from(x)).collect();

            for col in &doc_columns {
                if !new_dataframe.has_column(col) {

                    // If a new column is found, add it to the dataset.
                    dataset_columns.insert(col.to_string());

                    // Fill the columns with nulls, so if we insert a new column at length 8, 1to7 are not empty, but have nulls.
                    new_dataframe.add_column(col.to_string(), CColumn { values: vec![CValue::None; row_count], data_type: CValueType::Unknown, expected_dtype: CValueType::Unknown });
                }
            }

            for (col, value) in document {
                new_dataframe.add_value_to_column(&col, bson_to_cvalue(value))
            }

            for col in &dataset_columns {
                if !doc_columns.contains(&col) {
                    // If a document does not contain a key in the dataframe, add a None, so all
                    // columns have the same length.
                    new_dataframe.add_value_to_column(col, CValue::None)
                }
            }
            row_count += 1;
        }
        new_dataframe.count = row_count;
        new_dataframe.set_schema(schema);
        new_dataframe
    }

    pub fn has_column(&self, column_name: &String) -> bool {
        self.columns.contains_key(column_name)
    }

    pub fn entry(&mut self, key: String) -> Entry<'_, String, CColumn> {
        self.columns.entry(key)
    }

    pub fn add_column(&mut self, name: String, column: CColumn) {
        self.columns.insert(name, column);
    }

    pub fn add_value_to_column(&mut self, name: &String, value: CValue) {
        self.columns.get_mut(name).unwrap().values.push(value);
    }

    pub fn modify_column<Fn>(&mut self, name: String, func: Fn)
    where
        Fn: FnOnce(&mut CColumn),

    {
        self.columns.entry(name).and_modify(|x| { func(x) });
    }

    pub fn add_values_to_column(&mut self, name: String, values: Vec<CValue>) {
        self.columns.entry(name).and_modify(|x| { x.values.extend(values); });
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut CColumn> {
        self.columns.get_mut(key)
    }
}

#[derive(Debug)]
pub struct CColumn {
    pub values: Vec<CValue>,
    pub data_type: CValueType,
    pub expected_dtype: CValueType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CValue {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    Double32(f32),
    Double64(f64),
    String(String),
    VecString(Vec<String>),
    VecI32(Vec<i32>),
    VecI64(Vec<i64>),
    VecF32(Vec<f32>),
    VecF64(Vec<f64>),
    VecDyn(Vec<CValue>),
    Object(HashMap<String, CValue>),
    None,
    Unknown,
}


#[derive(PartialOrd, PartialEq, Debug, Clone, Serialize, Deserialize, Copy)]
pub enum CValueType {
    Bool,
    I16,
    I32,
    I64,
    Double32,
    Double64,
    String,
    VecString,
    VecI32,
    VecI64,
    VecF32,
    VecF64,
    VecDyn,
    Object,
    None,
    Unknown,
}

impl FromStr for CValueType {
    type Err = ParseCValueTypeError;

    fn from_str(input: &str) -> Result<CValueType, ParseCValueTypeError> {
        match input.to_lowercase().as_str() {
            "string" => Ok(CValueType::String),
            "vecstring" => Ok(CValueType::VecString),
            "i32" => Ok(CValueType::I32),
            "object" => Ok(CValueType::Object),
            _ => Err(ParseCValueTypeError { message: format!("'{}' is not a valid CValue", input) }),
        }
    }
}


#[derive(Debug, Clone)]
pub struct ParseCValueTypeError {
    message: String,
}
impl CValue {
    pub fn get_dtype(&self) -> CValueType {
        match self {
            Self::String(_) => CValueType::String,
            Self::I16(_) => CValueType::I16,
            Self::I32(_) => CValueType::I32,
            Self::I64(_) => CValueType::I64,
            Self::VecDyn(_) => CValueType::VecDyn,
            Self::Object(_) => CValueType::Object,
            Self::None => CValueType::None,
            Self::Unknown => CValueType::Unknown,

            _ => {
                CValueType::Unknown
            }
        }
    }

    pub fn equals_dtype(&self, other: &Self) -> bool {
        self.get_dtype() == other.get_dtype()
    }

    pub fn is_dtype(&self, _type: CValueType) -> bool {
        self.get_dtype() == _type
    }
}

impl fmt::Display for CValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => write!(f, "{:^10}", s),
            Self::I16(s) => write!(f, "{}", s),
            Self::I32(s) => write!(f, "{:^10}", s),
            Self::I64(s) => write!(f, "{}", s),
            Self::VecDyn(v) => write!(f, "{:?}", v),
            Self::Object(v) => write!(f, "{:?}", v),
            Self::None => write!(f, "   None   "),
            _ => write!(f, "")
        }
    }
}
impl fmt::Display for ParseCValueTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error parsing CValue: {}", self.message)
    }
}


pub fn get_inner_cvalue_type_name(value: &CValue) -> String {
    match value {
        CValue::String(_) => "str".to_string(),
        CValue::I64(_) => "i32".to_string(),
        CValue::VecF32(_) => "vec_f32".to_string(),
        CValue::I32(_) => "i32".to_string(),
        CValue::VecString(_) => "vecstring".to_string(),
        CValue::VecDyn(_) => "vecdyn".to_string(),
        CValue::Object(_) => "object".to_string(),
        _ => "none".to_string()
    }
}