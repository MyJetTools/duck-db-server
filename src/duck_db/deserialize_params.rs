use super::DuckDbValue;

pub fn deserialize_params(data: &[u8]) -> Vec<DuckDbValue> {
    let json_reader = my_json::json_reader::JsonArrayIterator::new(data).unwrap();

    let mut result = Vec::new();
    while let Some(json_value) = json_reader.get_next() {
        let json_value = json_value.unwrap();

        if json_value.is_number() {
            result.push(DuckDbValue::Number(
                json_value.unwrap_as_number().unwrap().unwrap(),
            ));
            continue;
        }
        if json_value.is_bool() {
            result.push(DuckDbValue::Bool(json_value.unwrap_as_bool().unwrap()));
            continue;
        }

        if json_value.is_double() {
            let value = json_value.unwrap_as_double().unwrap().unwrap();

            result.push(DuckDbValue::Double(value));
            continue;
        }

        if json_value.is_null() {
            result.push(DuckDbValue::Null);
            continue;
        }

        result.push(DuckDbValue::String(
            json_value.as_str().unwrap().to_string(),
        ));
    }

    result
}
