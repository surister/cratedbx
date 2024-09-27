use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub dtype: String,
    pub dtype_collision_strategy: String,
    pub sub_schema: Option<HashMap<String, ColumnInfo>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CSchema {
    #[serde(flatten)]
    pub columns: HashMap<String, ColumnInfo>,
}