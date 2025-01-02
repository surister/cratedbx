use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{write, Formatter};
use std::str::FromStr;
use indexmap::IndexMap;
use indexmap::map::Entry;
use mongodb::bson::{Document};
use serde::{Deserialize, Serialize, Serializer};
use crate::experiment::schema::CSchema;
use crate::experiment::trans::bson_to_cvalue;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DtypeStrategy {
    NewCol,
    Cast,
    Ignore,
    Remove,
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

fn shorten_string(s: &CValue, max_length: usize) -> String {
    if s.to_string().chars().count() > max_length {
        let shortened: String = s.to_string().chars().take(max_length - 3).collect();  // Take max_length - 3 to leave space for "..."
        format!("{}...", shortened)
    } else {
        s.to_string()
    }
}

/// Receives a VecDyn and returns its downcasted form.
fn downcast_vector(vector: &[CValue], target_dtype: CValueType) {}

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

    pub fn from_bson(documents: Vec<Document>, schema: Option<CSchema>) -> Self {
        let mut new_dataframe = Self::from_bson_typeless(documents);
        new_dataframe.apply_schema(schema);
        new_dataframe
    }

    pub fn from_bson_typeless(documents: Vec<Document>) -> Self {
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
                            sub_schema: None,
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
        println!("Schema:\n------");
        for (name, column) in &self.columns {
            println!("{:?} expected_dtype: {:?}, dtype: {:?}, strategy: {:?}, sub_schema: {:?}",
                     name,
                     column.expected_dtype,
                     column.data_type,
                     column.dtype_strategy,
                     column.sub_schema
            )
        }
    }

    pub fn print(&self, rows: Option<usize>) {
        let headers = if self.selected_columns.is_empty() {
            self.columns.iter().map(|(x, c)| x.to_string()).collect()
        } else {
            self.selected_columns.clone()
        };

        let count = if let Some(count) = rows {
            count
        } else {
            self.count
        };
        println!("\n{}", "+-----------".repeat(headers.len()));

        for key in &headers {
            print!("| {:^10}", key);
        }
        println!(); // Move to the next line.
        println!("{}", "|-----------".repeat(headers.len()));
        for i in 0..count {
            for key in &headers {
                let vec = &self.columns[key].values;
                if i < vec.len() {
                    print!("| {:^1} ", &vec[i]);
                } else {
                    print!("{:<10} ", " ");
                }
            }
            println!(); // Move to the next line after each row
        }
        println!("{}", "+-----------".repeat(headers.len()));
    }

    fn set_schema(&mut self, schema: CSchema) {
        println!();
        for (name, column) in schema.columns {
            if self.has_column(&name) {
                self.modify_column(name, |x| {
                    println!("{:?}", &column);
                    x.expected_dtype = column.dtype;
                    x.dtype_strategy = column.dtype_collision_strategy;
                    x.sub_schema = column.sub_schema
                });
            }
        }
        println!();
    }

    /// Applies the current schema, checking every value and resolving type mismatches with
    /// dtype_strategy; it optionally accepts a new schema that will be applied before.
    /// This is an expensive operation and should only be used when needed.
    pub fn apply_schema(&mut self, new_schema: Option<CSchema>) {
        if let Some(new_schema) = new_schema {
            self.set_schema(new_schema);
        }
        let mut new_rows: Vec<(CValue, usize, String)> = vec![];

        for (name, column) in
            self
                .columns
                .iter_mut()
                .filter(|(_, column)| column.expected_dtype != CValueType::Unknown && !column.is_expected_vector())
        {
            for (i, value) in column.values.iter_mut().enumerate() {

                if !&value.equals_dtype(&CValue::None) && !value.is_dtype(column.expected_dtype) {
                    match column.dtype_strategy {
                        DtypeStrategy::NewCol => {
                            new_rows.push((value.clone(), i, name.clone()));
                            *value = CValue::None;
                        }

                        DtypeStrategy::Cast => {
                            match (&column.expected_dtype, &value) {
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

        // Keep track of the last inserted value depth so we can calculate the new depth.
        let mut last_depth: usize = 0;

        for (value, depth, column_name) in new_rows {

            // Prefill the new col_values with nulls
            let mut col_values: Vec<CValue> = Vec::with_capacity(depth + 1);

            let new_column_name = format!("{}_{}", column_name, get_inner_cvalue_type_name(&value)).to_string();

            if self.has_column(&column_name) {
                let dtype = value.get_dtype();
                last_depth = depth;
                col_values.extend(vec![CValue::None; depth]);
                col_values.push(value);

                self.add_column(
                    new_column_name,
                    CColumn {
                        values: col_values,
                        expected_dtype: CValueType::Unknown,
                        data_type: dtype,
                        dtype_strategy: DtypeStrategy::Ignore,
                        sub_schema: None,
                    });
            } else {
                col_values.extend(vec![CValue::None; depth - last_depth]);
                col_values.push(value);

                self.add_values_to_column(new_column_name, col_values);
            }
        }

        // We repeat the process with columns that have CValueType::Object dtype.
        for (name, column) in
            self
                .columns
                .iter_mut()
                .filter(|(_, column)| column.expected_dtype == CValueType::Object && column.sub_schema != None)
        {
            for (i, value) in column.values.iter_mut().enumerate() {
                if let Some(expected_types) = &column.sub_schema {
                    match value {
                        CValue::Object(obj) => {
                            let mut new_rows = vec![];
                            for (k, v) in &mut *obj {
                                if expected_types.get(k) != Option::from(&v.get_dtype()) {
                                    new_rows.push((format!("{}_{}", name, get_inner_cvalue_type_name(&v)).to_string(), v.clone()));
                                    *v = CValue::None
                                }
                            }
                            for (column_name, value) in new_rows {
                                obj.entry(column_name).or_insert(value);
                            }
                        }
                        _ => continue
                    }
                }
            }
        }
        let mut value_map:HashMap<CValueType, CValue> = HashMap::new();
        let mut new_rows: Vec<(CValue, usize, String)> = vec![];
        // Downcast vecdyns
        for (name, mut column) in
            self
                .columns
                .iter_mut()
                .filter(|(_, column)| column.is_expected_vector())
        {
            for (i, value) in column.values.iter_mut().enumerate() {
                let mut depth = 0;
                match value {
                    CValue::VecDyn(v) => {
                        for v1 in v {
                            match v1 {
                                CValue::String(v) => {
                                    value_map.entry(CValueType::VecString).and_modify(|x| {
                                        match x {
                                            CValue::VecString(s) => {
                                                s.push(v.clone());
                                            }
                                            _ => ()
                                        }
                                    }).or_insert(CValue::VecString(vec![v.to_string()]));
                                },
                                CValue::I32(v) => {
                                    value_map.entry(CValueType::I32).and_modify(|x| {
                                        match x {
                                            CValue::VecI32(s) => {
                                                s.push(*v);
                                            }
                                            _ => ()
                                        }
                                    }).or_insert(CValue::VecI32(vec![*v]));
                                },
                                _ => continue
                            }
                            depth += 1;
                        }
                    }
                    _ => continue
                }

                let (dtype, replaced_column) = value_map.remove_entry(&column.expected_dtype).unwrap();
                column.data_type = dtype;
                *value = replaced_column;
            }


        }

        for (k, v) in value_map {
            let mut new_v = vec![CValue::None; 11];
            new_v.push(v);
            self.add_column("some_vec_i32".to_string(), CColumn { values: new_v, data_type: k, expected_dtype: k, dtype_strategy: DtypeStrategy::Ignore, sub_schema: None })
        }


        // Right side None fill.
        for column in self.columns.values_mut() {
            let none_count = self.count - column.values.len();
            column.values.extend(vec![CValue::None; none_count]);
        }
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
    /// The resolution strategy that will be applied on data types mismatches.
    pub dtype_strategy: DtypeStrategy,
    pub sub_schema: Option<HashMap<String, CValueType>>,
}

impl CColumn {
    pub fn is_vector() {
        return;
    }
    pub fn is_expected_vector(&self) -> bool {
        match self.expected_dtype {
            CValueType::VecDyn |
            CValueType::VecString |
            CValueType::VecI64 |
            CValueType::VecI32 |
            CValueType::VecF64 |
            CValueType::VecF32 => true,
            _ => false
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum CValue {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    U16(u16),
    U32(u32),
    U64(u64),
    Double32(f32),
    Double64(f64),
    String(String),
    VecString(Vec<String>), // This is to represent arrays
    VecI32(Vec<i32>),
    VecI64(Vec<i64>),
    VecF32(Vec<f32>),
    VecF64(Vec<f64>),
    VecDyn(Vec<CValue>),
    Object(HashMap<String, CValue>),
    None,
    Unknown,
}

impl Serialize for CValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CValue::I32(ref i) => i.serialize(serializer),
            CValue::I64(ref i) => i.serialize(serializer),
            CValue::Double64(i) => i.serialize(serializer),
            CValue::String(i) => i.serialize(serializer),
            CValue::U32(i) => i.serialize(serializer),
            CValue::U64(i) => i.serialize(serializer),
            _ => {
                32.serialize(serializer)
            }
        }
    }
}
#[derive(PartialOrd, PartialEq, Eq, Hash, Debug, Clone, Serialize, Deserialize, Copy)]
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
            Self::VecString(_) => CValueType::VecString,
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
            Self::VecI32(v) => write!(f, "{:?}", v),
            Self::VecDyn(v) => write!(f, "{:?}", v),
            Self::VecString(v) => write!(f, "{:?}", v),
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