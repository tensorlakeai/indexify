use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::Deserialize;

pub fn validate_label_key(key: &str) -> Result<()> {
    let validations = [
        (key.is_ascii(), "must be ASCII"),
        (key.len() <= 63, "must be 63 characters or less"),
        (
            key.chars()
                .next()
                .map_or(false, |c| c.is_ascii_alphanumeric()),
            "must begin with an alphanumeric character",
        ),
        (
            key.chars()
                .last()
                .map_or(false, |c| c.is_ascii_alphanumeric()),
            "must end with an alphanumeric character",
        ),
        (
            key.chars()
                .all(|c| c.is_ascii_alphanumeric() || ['-', '_', '.'].contains(&c)),
            "must contain only alphanumeric characters, dashes, underscores, and dots",
        ),
    ];

    let mut err_msgs = vec![];
    for (valid, msg) in validations.iter() {
        if !valid {
            err_msgs.push(*msg);
        }
    }

    if err_msgs.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "label key invalid - {} - found key : \"{}\"",
            err_msgs.join(", "),
            key
        ))
    }
}

pub fn validate_label_value(value: &str) -> Result<()> {
    // empty string is ok
    if value.is_empty() {
        return Ok(());
    }
    let validations = [
        (value.is_ascii(), "must be ASCII"),
        (value.len() <= 63, "must be 63 characters or less"),
        (
            value
                .chars()
                .next()
                .map_or(false, |c| c.is_ascii_alphanumeric()),
            "must begin with an alphanumeric character",
        ),
        (
            value
                .chars()
                .last()
                .map_or(false, |c| c.is_ascii_alphanumeric()),
            "must end with an alphanumeric character",
        ),
        (
            value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || ['-', '_', '.'].contains(&c)),
            "must contain only alphanumeric characters, dashes, underscores, and dots",
        ),
    ];

    let mut err_msgs = vec![];
    for (valid, msg) in validations.iter() {
        if !valid {
            err_msgs.push(*msg);
        }
    }

    if err_msgs.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "label value invalid - {} - found value : \"{}\"",
            err_msgs.join(", "),
            value
        ))
    }
}

pub fn parse_validate_label_raw(raw: &str) -> Result<(String, String)> {
    let mut split = raw.split(':');

    let mut err_msgs = vec![];
    let validations = [
        (split.clone().count() > 1, "must have a ':' character"),
        (
            split.clone().count() < 3,
            "must have only one ':' character",
        ),
    ];

    for (valid, msg) in validations.iter() {
        if !valid {
            err_msgs.push(*msg);
        }
    }

    if err_msgs.is_empty() {
        let key = split.next().unwrap_or("").to_string();
        let value = split.next().unwrap_or("").to_string();
        Ok((key, value))
    } else {
        Err(anyhow!(
            "query invalid - {} - raw : \"{}\"",
            err_msgs.join(", "),
            raw
        ))
    }
}

#[cfg(test)]
mod test_label_validation {
    use super::*;

    #[test]
    fn test_parse_validate_label_raw() {
        let invalid_raw_labels = vec!["", "key", "key:value:value2", "key:value:value2:value3"];

        let valid_raw_labels = vec![
            ("key:value", ("key".to_string(), "value".to_string())),
            ("key:", ("key".to_string(), "".to_string())),
            (":value", ("".to_string(), "value".to_string())),
        ];

        for label in invalid_raw_labels {
            let result = parse_validate_label_raw(label);
            assert!(result.is_err(), "should be invalid: {}", label);
        }

        for (label, expected) in valid_raw_labels {
            let result = parse_validate_label_raw(label);
            assert!(
                result.is_ok(),
                "should be valid: {} - {}",
                label,
                result.unwrap_err()
            );
            assert_eq!(result.unwrap(), expected);
        }
    }
    #[test]
    fn test_validate_label_key() {
        let too_long_string = "a".repeat(64);
        let good_length_string = "a".repeat(63);
        let invalid_keys = vec![
            "",
            "thumbs_up_emoji_👍",
            "& ^ % $ # @ !",
            " ",
            "_",
            "_-_.",
            too_long_string.as_str(),
        ];

        let valid_keys = vec![
            "key",
            "key1",
            "key-1",
            "key_1",
            "key.1",
            good_length_string.as_str(),
        ];

        for key in invalid_keys {
            let result = validate_label_key(key);
            assert!(result.is_err(), "should be invalid: {}", key);
        }

        for key in valid_keys {
            let result = validate_label_key(key);
            assert!(result.is_ok(), "should be valid: {}", key);
        }
    }

    #[test]
    fn test_validate_label_value() {
        let too_long_string = "a".repeat(64);
        let good_length_string = "a".repeat(63);
        let invalid_values = vec![
            "thumbs_up_emoji_👍",
            "& ^ % $ # @ !",
            " ",
            "_",
            "_-_.",
            too_long_string.as_str(),
        ];

        let valid_values = vec![
            "",
            "value",
            "value1",
            "value-1",
            "value_1",
            "value.1",
            good_length_string.as_str(),
        ];

        for value in invalid_values {
            let result = validate_label_value(value);
            assert!(result.is_err(), "should be invalid: {}", value);
        }

        for value in valid_values {
            let result = validate_label_value(value);
            assert!(result.is_ok(), "should be valid: {}", value);
        }
    }
}

pub fn deserialize_none_to_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = serde::Deserialize::deserialize(deserializer)?;
    Ok(s.unwrap_or("".to_string()))
}

pub fn deserialize_labels_eq_filter<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let err_formatter = |msg: String, e: String| -> D::Error {
        serde::de::Error::custom(format!("invalid labels_eq filter - {}: {}", msg, e))
    };

    // labels_eq is in the form labels_eq=key1:value1,key2:value2
    // split on comma
    let labels = String::deserialize(deserializer)?;
    let labels: Vec<&str> = labels.split(',').collect();

    // if there's one label and it's an empty string (i.e. labels_eq=)
    // this is invalid - if a user wants to match on no labels,
    // they should remove the labels_eq filter entirely
    if labels.len() == 1 && labels[0].is_empty() {
        return Err(err_formatter(
            "query invalid".to_string(),
            "must have at least one label - if you want to match on no labels, remove the labels_eq filter entirely".to_string(),
        ));
    }

    // if there are labels, then parse them
    let mut labels_eq = HashMap::new();
    for label in labels {
        let (key, value) = parse_validate_label_raw(label)
            .map_err(|e| err_formatter("query invalid".to_string(), e.to_string()))?;

        // if the key already exists, then it's a duplicate
        if labels_eq.contains_key(&key) {
            return Err(err_formatter(
                "query has duplicate key".to_string(),
                label.to_string(),
            ));
        }
        validate_label_key(key.as_str())
            .map_err(|e| err_formatter("key invalid".to_string(), e.to_string()))?;
        validate_label_value(value.as_str())
            .map_err(|e| err_formatter("value invalid".to_string(), e.to_string()))?;

        labels_eq.insert(key, value);
    }

    for (key, value) in labels_eq.clone() {
        // if the first part is empty, then it's invalid
        if key.is_empty() {
            return Err(serde::de::Error::custom(format!(
                "invalid labels_eq filter - must be in the form 'key:value' or 'key:' or ''"
            )));
        }
        // if the second part is empty, then it's an empty string value filter
        if value.is_empty() {
            continue;
        }
    }

    Ok(Some(labels_eq))
}

#[cfg(test)]
mod test_deserialize_labels_eq_filter {
    use axum::extract::Query;
    use hyper::Uri;

    use super::*;
    use crate::ListContentFilters;

    /// 1. ?source=foo&labels_eq=key:value
    #[test]
    fn test_key_value() {
        let expected_query: Query<ListContentFilters> = Query(ListContentFilters {
            source: "foo".to_string(),
            parent_id: "".to_string(),
            labels_eq: Some({
                let mut labels_eq = HashMap::new();
                labels_eq.insert("key".to_string(), "value".to_string());
                labels_eq
            }),
        });

        let query_str: Uri = "http://example.com/path?source=foo&labels_eq=key:value"
            .parse()
            .unwrap();
        let query: Query<ListContentFilters> = Query::try_from_uri(&query_str).unwrap();
        assert_eq!(query.0, expected_query.0);
    }

    /// 2. ?source=foo&labels_eq=key:
    #[test]
    fn test_key_empty_value() {
        let expected_query: Query<ListContentFilters> = Query(ListContentFilters {
            source: "foo".to_string(),
            parent_id: "".to_string(),
            labels_eq: Some({
                let mut labels_eq = HashMap::new();
                labels_eq.insert("key".to_string(), "".to_string());
                labels_eq
            }),
        });

        let query_str: Uri = "http://example.com/path?source=foo&labels_eq=key:"
            .parse()
            .unwrap();
        let query: Query<ListContentFilters> = Query::try_from_uri(&query_str).unwrap();
        assert_eq!(query.0, expected_query.0);
    }

    /// 3. ?source=foo&labels_eq=
    /// ?source=foo&labels_eq= is invalid and throws an error
    #[test]
    fn test_empty_is_invalid() {
        let query_str: Uri = "http://example.com/path?source=foo&labels_eq="
            .parse()
            .unwrap();
        let query: Result<Query<ListContentFilters>, _> = Query::try_from_uri(&query_str);
        assert!(query.is_err(), "query should be invalid: \"labels_eq=\"");
    }

    /// 4. ?source=foo&labels_eq=key:value&labels_eq=key2:value2
    #[test]
    fn test_multiple_key_value() {
        let expected_query: Query<ListContentFilters> = Query(ListContentFilters {
            source: "foo".to_string(),
            parent_id: "".to_string(),
            labels_eq: Some({
                let mut labels_eq = HashMap::new();
                labels_eq.insert("key".to_string(), "value".to_string());
                labels_eq.insert("key2".to_string(), "value2".to_string());
                labels_eq
            }),
        });

        let query_str: Uri = "http://example.com/path?source=foo&labels_eq=key:value,key2:value2"
            .parse()
            .unwrap();
        let query: Query<ListContentFilters> = Query::try_from_uri(&query_str).unwrap();
        assert_eq!(query.0, expected_query.0);
    }

    /// 5. ?source=foo&labels_eq=key:value&labels_eq=key2:
    #[test]
    fn test_multiple_key_value_key_empty_value() {
        let expected_query: Query<ListContentFilters> = Query(ListContentFilters {
            source: "foo".to_string(),
            parent_id: "".to_string(),
            labels_eq: Some({
                let mut labels_eq = HashMap::new();
                labels_eq.insert("key".to_string(), "value".to_string());
                labels_eq.insert("key2".to_string(), "".to_string());
                labels_eq
            }),
        });

        let query_str: Uri = "http://example.com/path?source=foo&labels_eq=key:value,key2:"
            .parse()
            .unwrap();
        let query: Query<ListContentFilters> = Query::try_from_uri(&query_str).unwrap();
        assert_eq!(query.0, expected_query.0);
    }

    /// INVALID - each of these throws error
    #[test]
    fn test_invalid() {
        let invalid_query_params: Vec<Uri> = vec![
            "labels_eq=key:value:key2:value2",
            "labels_eq=key:value:key2",
            "labels_eq=:value",
            "labels_eq=key",
            "labels_eq=",
            "labels_eq=:",
            "labels_eq=key:value&labels_eq=key2:value2",
        ]
        .into_iter()
        .map(|s| format!("http://example.com/path?source=foo&{}", s))
        .map(|s| s.parse().unwrap())
        .collect();

        for query_str in invalid_query_params {
            let query: Result<Query<ListContentFilters>, _> = Query::try_from_uri(&query_str);
            assert!(query.is_err(), "query should be invalid: {}", query_str);
        }
    }
}
