use std::collections::BTreeMap;

use crate::data_model::{ExecutorMetadata, FunctionAllowlist};

/// Computed equivalence class for executors.
///
/// Two executors with the same class produce identical constraint check
/// results. Only static constraint-relevant properties are included: labels and
/// function allowlist. This mirrors Nomad's `ComputedClass`.
///
/// Used as a cache key for feasibility checks — when one executor of a class
/// fails a constraint check, all other executors of the same class can be
/// skipped without evaluation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ExecutorClass {
    pub labels: BTreeMap<String, String>,
    /// Sorted allowlist. `None` = accept all functions.
    pub allowlist: Option<Vec<FunctionAllowlist>>,
}

impl ExecutorClass {
    /// Compute the equivalence class for an executor.
    pub fn from_executor(executor: &ExecutorMetadata) -> Self {
        let labels = executor
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let allowlist = executor.function_allowlist.as_ref().map(|list| {
            let mut sorted = list.clone();
            sorted.sort();
            sorted
        });
        Self { labels, allowlist }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn make_executor(
        labels: HashMap<String, String>,
        allowlist: Option<Vec<FunctionAllowlist>>,
    ) -> ExecutorMetadata {
        use crate::data_model::{ExecutorMetadataBuilder, HostResources};

        ExecutorMetadataBuilder::default()
            .id(crate::data_model::ExecutorId::new(
                "test-executor".to_string(),
            ))
            .executor_version("0.3.0".to_string())
            .function_allowlist(allowlist)
            .addr("127.0.0.1:8080".to_string())
            .labels(labels.into())
            .containers(imbl::HashMap::new())
            .host_resources(HostResources::default())
            .state(crate::data_model::ExecutorState::Running)
            .state_hash(String::new())
            .build()
            .unwrap()
    }

    #[test]
    fn test_same_labels_same_class() {
        let mut labels = HashMap::new();
        labels.insert("gpu".to_string(), "true".to_string());
        labels.insert("region".to_string(), "us-east".to_string());

        let exec1 = make_executor(labels.clone(), None);
        let exec2 = make_executor(labels, None);

        let class1 = ExecutorClass::from_executor(&exec1);
        let class2 = ExecutorClass::from_executor(&exec2);
        assert_eq!(class1, class2);
    }

    #[test]
    fn test_different_labels_different_class() {
        let mut labels1 = HashMap::new();
        labels1.insert("gpu".to_string(), "true".to_string());
        let mut labels2 = HashMap::new();
        labels2.insert("gpu".to_string(), "false".to_string());

        let exec1 = make_executor(labels1, None);
        let exec2 = make_executor(labels2, None);

        let class1 = ExecutorClass::from_executor(&exec1);
        let class2 = ExecutorClass::from_executor(&exec2);
        assert_ne!(class1, class2);
    }

    #[test]
    fn test_allowlist_order_independent() {
        let al1 = vec![
            FunctionAllowlist {
                namespace: Some("ns1".to_string()),
                application: None,
                function: None,
            },
            FunctionAllowlist {
                namespace: Some("ns2".to_string()),
                application: None,
                function: None,
            },
        ];
        let al2 = vec![
            FunctionAllowlist {
                namespace: Some("ns2".to_string()),
                application: None,
                function: None,
            },
            FunctionAllowlist {
                namespace: Some("ns1".to_string()),
                application: None,
                function: None,
            },
        ];

        let exec1 = make_executor(HashMap::new(), Some(al1));
        let exec2 = make_executor(HashMap::new(), Some(al2));

        let class1 = ExecutorClass::from_executor(&exec1);
        let class2 = ExecutorClass::from_executor(&exec2);
        assert_eq!(class1, class2);
    }

    #[test]
    fn test_none_allowlist_vs_some_different_class() {
        let exec1 = make_executor(HashMap::new(), None);
        let exec2 = make_executor(
            HashMap::new(),
            Some(vec![FunctionAllowlist {
                namespace: Some("ns1".to_string()),
                application: None,
                function: None,
            }]),
        );

        let class1 = ExecutorClass::from_executor(&exec1);
        let class2 = ExecutorClass::from_executor(&exec2);
        assert_ne!(class1, class2);
    }
}
