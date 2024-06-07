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
    let value = serde_json::from_str(&serde_json::to_string(&prost_json)?)?;
    match &value {
        serde_json::Value::Number(n) if n.as_f64().map_or(false, |n| n.fract() == 0.0) => Ok(
            serde_json::Value::Number((n.as_f64().unwrap().round() as i64).into()),
        ),
        _ => Ok(value),
    }
}

pub fn convert_map<T, V, F>(map: HashMap<String, T>, f: F) -> Result<HashMap<String, V>>
where
    F: Fn(T) -> Result<V>,
{
    map.into_iter().map(|(k, v)| Ok((k, f(v)?))).collect()
}

pub fn convert_map_serde_to_prost_json(
    map: HashMap<String, serde_json::Value>,
) -> Result<HashMap<String, prost_wkt_types::Value>> {
    convert_map(map, convert_serde_to_prost_json)
}

pub fn convert_map_prost_to_serde_json(
    map: HashMap<String, prost_wkt_types::Value>,
) -> Result<HashMap<String, serde_json::Value>> {
    convert_map(map, convert_prost_to_serde_json)
}
