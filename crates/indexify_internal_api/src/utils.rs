use std::collections::HashMap;

use anyhow::Result;

pub fn convert_serde_to_prost_json(
    serde_json: serde_json::Value,
) -> Result<prost_wkt_types::Value> {
    let value: prost_wkt_types::Value = serde_json::from_value(serde_json)?;
    Ok(value)
}

pub fn convert_prost_to_serde_json(
    prost_json: prost_wkt_types::Value,
) -> Result<serde_json::Value> {
    let string_value = serde_json::to_string(&prost_json)?;
    let value = serde_json::from_str(&string_value)?;

    // If the value is a float and has a trailing zero, convert it to an integer.
    // This is required because the prost_wkt_types::Value only supports floats.
    if let serde_json::Value::Number(n) = &value {
        if let Some(n) = &n.as_f64() {
            let string_float = format!("{n:.1}");
            let last_number = string_float.chars().last().unwrap();
            if last_number == '0' {
                let int = n.round() as i64;
                let value = serde_json::Value::Number(int.into());
                return Ok(value);
            }
        }
    }

    Ok(value)
}

pub fn convert_map_serde_to_prost_json(
    map: HashMap<String, serde_json::Value>,
) -> Result<HashMap<String, prost_wkt_types::Value>> {
    let mut new_map = std::collections::HashMap::new();
    for (key, value) in map {
        let new_value = convert_serde_to_prost_json(value)?;
        new_map.insert(key, new_value);
    }

    Ok(new_map)
}

pub fn convert_map_prost_to_serde_json(
    map: HashMap<String, prost_wkt_types::Value>,
) -> Result<HashMap<String, serde_json::Value>> {
    let mut new_map = std::collections::HashMap::new();
    for (key, value) in map {
        let new_value = convert_prost_to_serde_json(value)?;
        new_map.insert(key, new_value);
    }

    Ok(new_map)
}
