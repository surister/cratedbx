use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::experiment::data::CValueType;

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub dtype: CValueType,
    pub dtype_collision_strategy: String,
    pub sub_schema: Option<HashMap<String, ColumnInfo>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CSchema {
    #[serde(flatten)]
    pub columns: HashMap<String, ColumnInfo>,
}