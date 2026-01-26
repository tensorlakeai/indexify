use std::{
    collections::HashMap,
    fmt::{self, Display},
};

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
    pub fn matches(&self, values: &HashMap<String, String>) -> bool {
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

    /// Match labels with additional constraint expressions applied dynamically.
    /// This is used to inject placement constraints at matching time rather
    /// than storing them in the object. For example, with constraints coming
    /// from the ServerConfig.
    ///
    /// The additional constraints are parsed from expression strings and must
    /// all match. Any existing constraints in the filter that share keys
    /// with additional constraints are ignored (the additional constraints
    /// take precedence).
    pub fn matches_with_additional_constraints(
        &self,
        values: &HashMap<String, String>,
        additional_constraints: &LabelsFilter,
    ) -> bool {
        // First, collect keys from additional constraints
        let additional_constraints_keys: std::collections::HashSet<String> = additional_constraints
            .0
            .iter()
            .map(|expr| expr.key.clone())
            .collect();

        // Check all additional constraints match
        for expr in &additional_constraints.0 {
            let value = values.get(&expr.key);
            let matches = match value {
                Some(v) => match expr.operator {
                    Operator::Eq => v == &expr.value,
                    Operator::Neq => v != &expr.value,
                },
                None => false,
            };
            if !matches {
                return false;
            }
        }

        // Check existing constraints, skipping those overridden by additional
        // constraints
        for expr in &self.0 {
            if additional_constraints_keys.contains(&expr.key) {
                continue; // Skip, additional constraint takes precedence
            }
            let value = values.get(&expr.key);
            let matches = match value {
                Some(v) => match expr.operator {
                    Operator::Eq => v == &expr.value,
                    Operator::Neq => v != &expr.value,
                },
                None => false,
            };
            if !matches {
                return false;
            }
        }

        true
    }
}

impl From<Vec<String>> for LabelsFilter {
    fn from(value: Vec<String>) -> Self {
        Self(
            value
                .iter()
                .map(|v| Expression::try_from_str(v).unwrap())
                .collect(),
        )
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

        let mut values = HashMap::new();
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
        let empty_labels = HashMap::new();
        assert!(empty_filter.matches(&empty_labels));

        // Empty filter should also match non-empty labels
        let mut labels = HashMap::new();
        labels.insert("any_key".to_string(), "any_value".to_string());
        assert!(empty_filter.matches(&labels));
    }

    #[test]
    fn test_matches_with_additional_constraints_basic() {
        let filter: LabelsFilter = vec!["region==us-west".to_string()].into();

        let mut values = HashMap::new();
        values.insert("region".to_string(), "us-west".to_string());

        // No additional constraints - should match
        assert!(filter.matches_with_additional_constraints(&values, &LabelsFilter::default()));

        // Add matching additional constraint
        assert!(
            filter
                .matches_with_additional_constraints(&values, &vec!["zone==1".to_string()].into())
        );

        // Add zone to values
        values.insert("zone".to_string(), "1".to_string());
        assert!(
            filter
                .matches_with_additional_constraints(&values, &vec!["zone==1".to_string()].into())
        );

        // Additional constraint doesn't match
        assert!(
            !filter
                .matches_with_additional_constraints(&values, &vec!["zone==2".to_string()].into())
        );
    }

    #[test]
    fn test_matches_with_additional_constraints_override() {
        let filter = LabelsFilter(vec![Expression {
            key: "region".to_string(),
            value: "us-west".to_string(),
            operator: Operator::Eq,
        }]);

        let mut values = HashMap::new();
        values.insert("region".to_string(), "us-east".to_string());

        // Base filter shouldn't match (us-east != us-west)
        assert!(!filter.matches(&values));

        // With additional constraint overriding region to us-east, should match
        assert!(filter.matches_with_additional_constraints(
            &values,
            &vec!["region==us-east".to_string()].into()
        ));

        // Override with non-matching value
        assert!(!filter.matches_with_additional_constraints(
            &values,
            &vec!["region==us-west".to_string()].into()
        ));
    }

    #[test]
    fn test_matches_with_additional_constraints_missing_key_eq() {
        let filter = LabelsFilter::default();

        let values = HashMap::new();

        // Eq constraint on missing key should return false
        assert!(!filter.matches_with_additional_constraints(
            &values,
            &vec!["missing==value".to_string()].into()
        ));
    }
}
