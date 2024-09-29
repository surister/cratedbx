use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Formatter;
use std::str::FromStr;
use indexmap::IndexMap;
use indexmap::map::Entry;
use mongodb::bson::{Bson, Document};
use serde::{Deserialize, Serialize};
use crate::experiment::schema::CSchema;
use crate::experiment::trans::bson_to_cvalue;

#[derive(Debug)]
pub enum DtypeStrategy {
    new_col,
    cast,
    ignore,
    remove,
}

pub struct ParseDtypeStrategyError {
    message: String,
}

impl FromStr for DtypeStrategy {
    type Err = ParseDtypeStrategyError;


    fn from_str(input: &str) -> Result<DtypeStrategy, ParseDtypeStrategyError> {
        match input.to_lowercase().as_str() {
            "new_col" => Ok(DtypeStrategy::new_col),
            "cast" => Ok(DtypeStrategy::cast),
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
            selected_columns: vec![]
        }
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
            println!("{:?} expected_dtype: {:?}, dtype: {:?}", name, column.expected_dtype, column.data_type)
        }
    }

    pub fn print(&self) {
        println!("{:?}", self.columns);
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

                    // Fill the columns with nulls.
                    // dataset.entry(col.to_string()).or_insert_with(|| vec![CValue::None; row_count]);
                    new_dataframe.add_column(col.to_string(), CColumn { values: vec![CValue::None; row_count], data_type: CValue::Unknown, expected_dtype: CValue::Unknown });
                }
            }

            for (col, value) in document {
                new_dataframe.add_value_to_column(&col, bson_to_cvalue(value))
            }

            for col in &dataset_columns {
                if !doc_columns.contains(&col) {
                    new_dataframe.add_value_to_column(col, CValue::None)
                }
            }
            row_count += 1;
        }
        new_dataframe.count = row_count;

        // for (name, col) in &new_dataframe.columns {
        //     let expected_dtype: Option<CValue> = get_expected_dtype(&schema, column);
        // }
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

    pub fn add_value_to_column(&mut self, name: &String, value: CValue){
        self.columns.get_mut(name).unwrap().values.push(value);
    }

    pub fn add_values_to_column(&mut self, name: String, values: Vec<CValue>){
         self.columns.entry(name).and_modify(|x| { x.values.extend(values); });
    }
    pub fn get_mut(&mut self, key: &str) -> Option<&mut CColumn> {
        self.columns.get_mut(key)
    }
}

#[derive(Debug)]
pub struct CColumn {
    pub(crate) values: Vec<CValue>,
    pub(crate) data_type: CValue,
    pub(crate) expected_dtype: CValue,
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


#[derive(PartialOrd, PartialEq)]
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

#[derive(Debug, Clone)]
pub struct ParseCValueError {
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
impl fmt::Display for ParseCValueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error parsing CValue: {}", self.message)
    }
}


impl FromStr for CValue {
    type Err = ParseCValueError;

    fn from_str(input: &str) -> Result<CValue, ParseCValueError> {
        match input.to_lowercase().as_str() {
            "string" => Ok(CValue::String("_".parse().unwrap())),
            "vecstring" => Ok(CValue::VecString(vec![])),
            "i32" => Ok(CValue::I32(0)),
            _ => Err(ParseCValueError { message: format!("'{}' is not a valid CValue", input) }),
        }
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