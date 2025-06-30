use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self, Display},
};

use anyhow::Result;
use serde::{de::Deserializer, Deserialize, Serialize, Serializer};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Neq,
    Gt,
    Lt,
    GtEq,
    LtEq,
}

impl Operator {
    pub fn from_str(operator: &str) -> Result<Self> {
        match operator {
            "=" => Ok(Self::Eq),
            "!=" => Ok(Self::Neq),
            ">" => Ok(Self::Gt),
            "<" => Ok(Self::Lt),
            ">=" => Ok(Self::GtEq),
            "<=" => Ok(Self::LtEq),
            _ => Err(anyhow::anyhow!("Invalid filter operator: {}", operator)),
        }
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Operator::Eq => "=",
                Operator::Neq => "!=",
                Operator::Gt => ">",
                Operator::Lt => "<",
                Operator::GtEq => ">=",
                Operator::LtEq => "<=",
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expression {
    pub key: String,
    pub value: Value,
    pub operator: Operator,
}

impl Serialize for Expression {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{}", self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Expression {
    fn deserialize<D>(deserializer: D) -> Result<Expression, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Expression::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Expression {
    pub fn from_str(str: &str) -> Result<Self> {
        // This parser must start with the longest operators first.
        let operators = vec!["!=", ">=", "<=", "=", ">", "<"];
        for operator in operators {
            let parts: Vec<&str> = str.split(operator).collect();
            if parts.len() != 2 {
                continue;
            }

            let key = parts[0].to_string();
            let value = serde_json::from_str(parts[1]).unwrap_or(serde_json::json!(parts[1]));
            let operator = Operator::from_str(operator)?;
            return Ok(Self {
                key,
                value,
                operator,
            });
        }
        Err(anyhow::anyhow!("Invalid filter: {}", str))
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.key, self.operator, self.value)
    }
}

fn partial_cmp(lhs: &Value, rhs: &Value) -> Option<Ordering> {
    match (lhs, rhs) {
        (Value::Number(n), Value::Number(m)) => n.as_f64()?.partial_cmp(&m.as_f64()?),
        (Value::String(s), Value::String(t)) => s.partial_cmp(t),
        (Value::Bool(b), Value::Bool(c)) => b.partial_cmp(c),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct LabelsFilter(pub Vec<Expression>);

impl LabelsFilter {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn expressions(&self) -> &[Expression] {
        &self.0
    }

    pub fn matches(&self, values: &HashMap<String, Value>) -> bool {
        self.0.iter().all(|expr| {
            let value = values.get(&expr.key);
            match value {
                Some(value) => match partial_cmp(value, &expr.value) {
                    Some(ordering) => match expr.operator {
                        Operator::Eq => ordering == std::cmp::Ordering::Equal,
                        Operator::Neq => ordering != std::cmp::Ordering::Equal,
                        Operator::Gt => ordering == std::cmp::Ordering::Greater,
                        Operator::Lt => ordering == std::cmp::Ordering::Less,
                        Operator::GtEq => ordering != std::cmp::Ordering::Less,
                        Operator::LtEq => ordering != std::cmp::Ordering::Greater,
                    },
                    None => false,
                },
                None => false,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        let filter = Expression::from_str("key=value").unwrap();
        assert_eq!(filter.operator, Operator::Eq);
        assert_eq!(filter.key, "key");
        assert_eq!(filter.value, serde_json::json!("value"));

        let filter_str = filter.to_string();
        assert_eq!(filter_str, "key=\"value\"");

        let filter = Expression::from_str("key>1").unwrap();
        assert_eq!(filter.operator, Operator::Gt);
        assert_eq!(filter.key, "key");
        assert_eq!(filter.value, serde_json::json!(1));

        let filter_str = filter.to_string();
        assert_eq!(filter_str, "key>1");

        let filter = Expression::from_str("key>=\"value\"").unwrap();
        assert_eq!(filter.operator, Operator::GtEq);
        assert_eq!(filter.key, "key");
        assert_eq!(filter.value, serde_json::json!("value"));

        let filter_str = filter.to_string();
        assert_eq!(filter_str, "key>=\"value\"");
    }

    #[test]
    fn test_matches() {
        let filter = LabelsFilter(vec![
            Expression {
                key: "key1".to_string(),
                value: serde_json::json!(1),
                operator: Operator::Eq,
            },
            Expression {
                key: "key2".to_string(),
                value: serde_json::json!(2),
                operator: Operator::Gt,
            },
        ]);

        let mut values = HashMap::new();
        values.insert("key1".to_string(), serde_json::json!(1));
        assert!(!filter.matches(&values));

        values.insert("key2".to_string(), serde_json::json!(2));
        assert!(!filter.matches(&values));

        values.insert("key2".to_string(), serde_json::json!(3));
        assert!(filter.matches(&values));
    }
}
