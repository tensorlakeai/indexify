use std::collections::HashMap;

use indexify_proto::indexify_coordinator;

/// filter for content metadata
pub fn list_content_filter<'a>(
    content: impl IntoIterator<Item = indexify_coordinator::ContentMetadata> + 'a,
    source: &'a str,
    parent_id: &'a str,
    labels_eq: &'a HashMap<String, String>,
) -> impl Iterator<Item = indexify_coordinator::ContentMetadata> + 'a {
    let content_vec: Vec<_> = content.into_iter().collect();
    content_vec
        .into_iter()
        .filter(move |c| source.is_empty() || c.source == source)
        .filter(move |c| {
            parent_id.is_empty() || c.parent_id.clone().unwrap_or_default().to_string() == parent_id
        })
        // c.labels HashMap<String, String> must exactly match labels_eq { ("key", "value") }
        // and not have any other labels
        .filter(move |c| {
            // if labels_eq is an empty hash map, skip the filter
            if labels_eq.is_empty() {
                return true;
            }
            // if there are labels_eq, then all labels must match
            if c.labels != *labels_eq {
                return false;
            }
            true
        })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_list_content_filter() {
        let no_labels_filter = HashMap::new();
        let content = vec![
            internal_api::ContentMetadata {
                id: ContentMetadataId {
                    id: "1".to_string(),
                    ..Default::default()
                },
                source: "source1".to_string(),
                parent_id: Some(ContentMetadataId {
                    id: "parent1".to_string(),
                    ..Default::default()
                }),
                labels: {
                    let mut labels = HashMap::new();
                    labels.insert("key1".to_string(), "value1".to_string());
                    labels
                },
                ..Default::default()
            },
            internal_api::ContentMetadata {
                id: ContentMetadataId {
                    id: "2".to_string(),
                    ..Default::default()
                },
                source: "source2".to_string(),
                parent_id: Some(ContentMetadataId {
                    id: "parent2".to_string(),
                    ..Default::default()
                }),
                labels: HashMap::new(),
                ..Default::default()
            },
            internal_api::ContentMetadata {
                id: ContentMetadataId {
                    id: "3".to_string(),
                    ..Default::default()
                },
                source: "source1".to_string(),
                parent_id: Some(ContentMetadataId {
                    id: "parent2".to_string(),
                    ..Default::default()
                }),
                labels: {
                    let mut labels = HashMap::new();
                    labels.insert("key1".to_string(), "value1".to_string());
                    labels.insert("key2".to_string(), "value2".to_string());
                    labels
                },
                ..Default::default()
            },
            internal_api::ContentMetadata {
                id: ContentMetadataId {
                    id: "4".to_string(),
                    ..Default::default()
                },
                source: "source4".to_string(),
                parent_id: Some(ContentMetadataId {
                    id: "parent4".to_string(),
                    ..Default::default()
                }),
                labels: HashMap::new(),
                ..Default::default()
            },
        ];

        // no filters
        let filtered_content =
            list_content_filter(content.clone(), "", "", &no_labels_filter).collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 4);
        assert_eq!(filtered_content[0].id, "1");
        assert_eq!(filtered_content[1].id, "2");
        assert_eq!(filtered_content[2].id, "3");
        assert_eq!(filtered_content[3].id, "4");

        // source filter
        let filtered_content =
            list_content_filter(content.clone(), "source1", "", &no_labels_filter)
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 2);
        assert_eq!(filtered_content[0].id, "1");
        assert_eq!(filtered_content[1].id, "3");

        // parent_id and source filter
        let filtered_content =
            list_content_filter(content.clone(), "source1", "parent2", &no_labels_filter)
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 1);
        assert_eq!(filtered_content[0].id, "3");

        // parent_id filter
        let filtered_content =
            list_content_filter(content.clone(), "", "parent2", &no_labels_filter)
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 2);
        assert_eq!(filtered_content[0].id, "2");
        assert_eq!(filtered_content[1].id, "3");

        // labels filter - empty - skips the labels filter
        let filtered_content =
            list_content_filter(content.clone(), "", "", &no_labels_filter).collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 4);

        // labels filter - exact match
        let labels_eq = {
            let mut labels = HashMap::new();
            labels.insert("key1".to_string(), "value1".to_string());
            labels
        };
        let filtered_content =
            list_content_filter(content.clone(), "", "", &labels_eq).collect::<Vec<_>>();
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
            list_content_filter(content.clone(), "", "", &labels_eq).collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 1);
        assert_eq!(filtered_content[0].id, "3");
    }
}

/// Returns true if the extractor supports the content mime type
/// Returns false if the extractor does not support the content mime type
/// Returns an error if the extractor is unable to fetch the ExtractorSchema
///
/// If the extractor input mime types include ["*/*"], then the extractor
/// supports all mime types. This is useful for debugging, or for
/// extractors that do not depend on the content mime type. However,
/// this is not recommended for production use as it can lead to
/// unexpected behavior.
///
/// Otherwise, the extractor input mime types are checked to see if they
/// include the content mime type.
///
/// For example, if the extractor input mime types are ["text/plain",
/// "application/pdf"], and the content mime type is "text/plain", then
/// the extractor supports the content mime type. Conversely, if the
/// extractor input mime types are ["text/plain", "application/pdf"], and
/// the content mime type is "image/png", then the extractor does not
/// support the content mime type and false is returned.
pub fn matches_mime_type(supported_mimes: &[String], content_mime_type: &String) -> bool {
    // if the extractor input mime types include ["*/*"], then the extractor
    // supports all mime types.
    if supported_mimes.contains(&mime::STAR_STAR.to_string()) {
        return true;
    }

    // otherwise, check if the extractor supports the content mime type
    supported_mimes.contains(content_mime_type)
}

#[cfg(test)]
mod test_extractor_mimetype_filter {
    use super::*;

    #[test]
    fn test_matches_mime_type() {
        // Assert that content with mime type matches an extractor that supports the
        // same mime type
        let res = matches_mime_type(
            &[
                mime::TEXT_PLAIN.to_string(),
                mime::IMAGE_PNG.to_string(),
                mime::APPLICATION_PDF.to_string(),
            ],
            &mime::TEXT_PLAIN.to_string(),
        );
        assert!(res);

        // Assert that content with mime type matches an extractor that supports wild
        // card mime type
        let res = matches_mime_type(
            &[mime::STAR_STAR.to_string()],
            &mime::TEXT_PLAIN.to_string(),
        );
        assert!(res);

        // Assert that content with mime type does not match an extractor that supports
        // the same mime type
        let res = matches_mime_type(
            &[mime::TEXT_PLAIN.to_string()],
            &mime::APPLICATION_PDF.to_string(),
        );
        assert!(!res);
    }
}
