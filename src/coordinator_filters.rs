use std::collections::HashMap;

use crate::internal_api::ContentMetadata;

/// filter for content metadata
pub fn list_content_filter<'a>(
    content: impl IntoIterator<Item = ContentMetadata> + 'a,
    source: &'a str,
    parent_id: &'a str,
    labels_eq: Option<&'a HashMap<String, String>>,
) -> impl Iterator<Item = ContentMetadata> + 'a {
    content
        .into_iter()
        .filter(move |c| source.is_empty() || c.source == source)
        .filter(move |c| parent_id.is_empty() || c.parent_id == parent_id)
        // c.labels HashMap<String, String> must exactly match labels_eq { ("key", "value") }
        // and not have any other labels
        .filter(move |c| {
            // if there's no labels_eq, then all labels are ok
            if labels_eq.is_none() {
                return true;
            }
            let labels_eq = labels_eq.unwrap();
            // if there are labels_eq, then all labels must match
            if c.labels != *labels_eq {
                return false;
            }
            true
        })
}

#[cfg(test)]
mod test_list_content_filter {
    use std::collections::HashMap;

    use super::*;
    use crate::state::store::SledStorableTestFactory;

    #[test]
    fn test_list_content_filter() {
        let default = ContentMetadata::spawn_instance_for_store_test();
        let content = vec![
            ContentMetadata {
                id: "1".to_string(),
                source: "source1".to_string(),
                parent_id: "parent1".to_string(),
                labels: {
                    let mut labels = HashMap::new();
                    labels.insert("key1".to_string(), "value1".to_string());
                    labels
                },
                ..default.clone()
            },
            ContentMetadata {
                id: "2".to_string(),
                source: "source2".to_string(),
                parent_id: "parent2".to_string(),
                labels: HashMap::new(),
                ..default.clone()
            },
            ContentMetadata {
                id: "3".to_string(),
                source: "source1".to_string(),
                parent_id: "parent2".to_string(),
                labels: {
                    let mut labels = HashMap::new();
                    labels.insert("key1".to_string(), "value1".to_string());
                    labels.insert("key2".to_string(), "value2".to_string());
                    labels
                },
                ..default.clone()
            },
            ContentMetadata {
                id: "4".to_string(),
                source: "source4".to_string(),
                parent_id: "parent4".to_string(),
                labels: HashMap::new(),
                ..default.clone()
            },
        ];

        // no filters
        let filtered_content = list_content_filter(content.clone().into_iter(), "", "", None)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 4);
        assert_eq!(filtered_content[0].id, "1");
        assert_eq!(filtered_content[1].id, "2");
        assert_eq!(filtered_content[2].id, "3");
        assert_eq!(filtered_content[3].id, "4");

        // source filter
        let filtered_content =
            list_content_filter(content.clone().into_iter(), "source1", "", None)
                .into_iter()
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 2);
        assert_eq!(filtered_content[0].id, "1");
        assert_eq!(filtered_content[1].id, "3");

        // parent_id and source filter
        let filtered_content =
            list_content_filter(content.clone().into_iter(), "source1", "parent2", None)
                .into_iter()
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 1);
        assert_eq!(filtered_content[0].id, "3");

        // parent_id filter
        let filtered_content =
            list_content_filter(content.clone().into_iter(), "", "parent2", None)
                .into_iter()
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 2);
        assert_eq!(filtered_content[0].id, "2");
        assert_eq!(filtered_content[1].id, "3");

        // labels filter - empty - only matches if there are no labels
        let filtered_content = list_content_filter(
            content.clone().into_iter(),
            "",
            "",
            Some(&HashMap::new()),
        )
        .into_iter()
        .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 2);
        assert_eq!(filtered_content[0].id, "2");
        assert_eq!(filtered_content[1].id, "4");

        // labels filter - exact match
        let labels_eq = {
            let mut labels = HashMap::new();
            labels.insert("key1".to_string(), "value1".to_string());
            labels
        };
        let filtered_content =
            list_content_filter(content.clone().into_iter(), "", "", Some(&labels_eq))
                .into_iter()
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 1);
        assert_eq!(filtered_content[0].id, "1");

        // labels filter - exact match multiple labels
        let labels_eq = {
            let mut labels = HashMap::new();
            labels.insert("key1".to_string(), "value1".to_string());
            labels.insert("key2".to_string(), "value2".to_string());
            labels
        };
        let filtered_content =
            list_content_filter(content.clone().into_iter(), "", "", Some(&labels_eq))
                .into_iter()
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 1);
        assert_eq!(filtered_content[0].id, "3");
    }
}
