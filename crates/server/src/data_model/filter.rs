use std::fmt::{self, Display};

use anyhow::Result;
use serde::{Deserialize, Serialize, Serializer, de::Deserializer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Neq,
}

impl Operator {
    pub fn try_from_str(operator: &str) -> Result<Self> {
        match operator {
            "==" => Ok(Self::Eq),
            "!=" => Ok(Self::Neq),
            _ => Err(anyhow::anyhow!("Invalid filter operator: {operator}")),
        }
    }
}

impl TryFrom<&str> for Operator {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::try_from_str(value)
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Operator::Eq => "==",
                Operator::Neq => "!=",
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expression {
    pub key: String,
    pub value: String,
    pub operator: Operator,
}

impl Serialize for Expression {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{self}").serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Expression {
    fn deserialize<D>(deserializer: D) -> Result<Expression, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Expression::try_from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Expression {
    pub fn try_from_str(str: &str) -> Result<Self> {
        // This parser must start with the longest operators first (if
        // additional operators are added).
        let operators = vec!["!=", "=="];
        for operator in operators {
            let parts: Vec<&str> = str.split(operator).collect();
            if parts.len() != 2 {
                continue;
            }

            let key = parts[0].to_string();
            let value = parts[1].to_string();
            let operator = Operator::try_from_str(operator)?;
            return Ok(Self {
                key,
                value,
                operator,
            });
        }
        Err(anyhow::anyhow!("Invalid label filter: {str}"))
    }
}

impl TryFrom<&str> for Expression {
    type Error = anyhow::Error;

    fn try_from(str: &str) -> Result<Self, Self::Error> {
        Self::try_from_str(str)
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.key, self.operator, self.value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct LabelsFilter(pub Vec<Expression>);

impl LabelsFilter {
    pub fn matches(&self, values: &imbl::HashMap<String, String>) -> bool {
        self.0.iter().all(|expr| {
            let value = values.get(&expr.key);
            match value {
                Some(value) => match expr.operator {
                    Operator::Eq => value == &expr.value,
                    Operator::Neq => value != &expr.value,
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
        let filter = Expression::try_from_str("key==value").unwrap();
        assert_eq!(filter.operator, Operator::Eq);
        assert_eq!(filter.key, "key");
        assert_eq!(filter.value, "value");

        let filter_str = filter.to_string();
        assert_eq!(filter_str, "key==value");

        let filter = Expression::try_from_str("key!=value").unwrap();
        assert_eq!(filter.operator, Operator::Neq);
        assert_eq!(filter.key, "key");
        assert_eq!(filter.value, "value");

        let filter_str = filter.to_string();
        assert_eq!(filter_str, "key!=value");
    }

    #[test]
    fn test_matches() {
        let filter = LabelsFilter(vec![
            Expression {
                key: "key1".to_string(),
                value: "1".to_string(),
                operator: Operator::Eq,
            },
            Expression {
                key: "key2".to_string(),
                value: "test".to_string(),
                operator: Operator::Neq,
            },
        ]);

        let mut values = imbl::HashMap::new();
        values.insert("key1".to_string(), "1".to_string());
        assert!(!filter.matches(&values));

        values.insert("key2".to_string(), "test".to_string());
        assert!(!filter.matches(&values));

        values.insert("key2".to_string(), "other".to_string());
        assert!(filter.matches(&values));
    }

    #[test]
    fn test_empty_filter_matches_all() {
        let empty_filter = LabelsFilter::default();

        // Empty filter should match empty labels
        let empty_labels = imbl::HashMap::new();
        assert!(empty_filter.matches(&empty_labels));

        // Empty filter should also match non-empty labels
        let mut labels = imbl::HashMap::new();
        labels.insert("any_key".to_string(), "any_value".to_string());
        assert!(empty_filter.matches(&labels));
    }
}
