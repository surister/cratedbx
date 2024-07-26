pub fn get_fqn_table(schema: &str, table_name: &str) -> String {
    let mut query: String = String::from("");
    query.push_str(format!(r#"{schema}.{table_name}"#).as_str());
    return query;
    // if !schema.is_none() {
    //     let _schema = schema;
    //     // let _schema = schema.unwrap();
    //     query.push_str(format!(r#"{_schema}"."{table_name}"#).as_str());
    // } else {
    //     query.push_str(format!(r#"{table_name}"#).as_str());
    // }
    // return query;
}