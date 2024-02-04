use std::collections::HashMap;

use indexify_internal_api as internal_api;

/// filter for content metadata
pub fn list_content_filter<'a>(
    content: impl IntoIterator<Item = internal_api::ContentMetadata> + 'a,
    source: &'a str,
    parent_id: &'a str,
    labels_eq: &'a HashMap<String, String>,
) -> impl Iterator<Item = internal_api::ContentMetadata> + 'a {
    content
        .into_iter()
        .filter(move |c| source.is_empty() || c.source == source)
        .filter(move |c| parent_id.is_empty() || c.parent_id == parent_id)
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
                id: "1".to_string(),
                source: "source1".to_string(),
                parent_id: "parent1".to_string(),
                labels: {
                    let mut labels = HashMap::new();
                    labels.insert("key1".to_string(), "value1".to_string());
                    labels
                },
                ..Default::default()
            },
            internal_api::ContentMetadata {
                id: "2".to_string(),
                source: "source2".to_string(),
                parent_id: "parent2".to_string(),
                labels: HashMap::new(),
                ..Default::default()
            },
            internal_api::ContentMetadata {
                id: "3".to_string(),
                source: "source1".to_string(),
                parent_id: "parent2".to_string(),
                labels: {
                    let mut labels = HashMap::new();
                    labels.insert("key1".to_string(), "value1".to_string());
                    labels.insert("key2".to_string(), "value2".to_string());
                    labels
                },
                ..Default::default()
            },
            internal_api::ContentMetadata {
                id: "4".to_string(),
                source: "source4".to_string(),
                parent_id: "parent4".to_string(),
                labels: HashMap::new(),
                ..Default::default()
            },
        ];

        // no filters
        let filtered_content =
            list_content_filter(content.clone().into_iter(), "", "", &no_labels_filter)
                .into_iter()
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 4);
        assert_eq!(filtered_content[0].id, "1");
        assert_eq!(filtered_content[1].id, "2");
        assert_eq!(filtered_content[2].id, "3");
        assert_eq!(filtered_content[3].id, "4");

        // source filter
        let filtered_content = list_content_filter(
            content.clone().into_iter(),
            "source1",
            "",
            &no_labels_filter,
        )
        .into_iter()
        .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 2);
        assert_eq!(filtered_content[0].id, "1");
        assert_eq!(filtered_content[1].id, "3");

        // parent_id and source filter
        let filtered_content = list_content_filter(
            content.clone().into_iter(),
            "source1",
            "parent2",
            &no_labels_filter,
        )
        .into_iter()
        .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 1);
        assert_eq!(filtered_content[0].id, "3");

        // parent_id filter
        let filtered_content = list_content_filter(
            content.clone().into_iter(),
            "",
            "parent2",
            &no_labels_filter,
        )
        .into_iter()
        .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 2);
        assert_eq!(filtered_content[0].id, "2");
        assert_eq!(filtered_content[1].id, "3");

        // labels filter - empty - skips the labels filter
        let filtered_content =
            list_content_filter(content.clone().into_iter(), "", "", &no_labels_filter)
                .into_iter()
                .collect::<Vec<_>>();
        assert_eq!(filtered_content.len(), 4);

        // labels filter - exact match
        let labels_eq = {
            let mut labels = HashMap::new();
            labels.insert("key1".to_string(), "value1".to_string());
            labels
        };
        let filtered_content = list_content_filter(content.clone().into_iter(), "", "", &labels_eq)
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
        let filtered_content = list_content_filter(content.clone().into_iter(), "", "", &labels_eq)
            .into_iter()
            .collect::<Vec<_>>();
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
    use crate::extractor::{Extractor, ExtractorSchema};

    #[derive(Debug)]
    enum TestExtractor {
        // "text/plain"
        TextPlain,

        // "*/*"
        Wildcard,
    }

    impl Extractor for TestExtractor {
        fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error> {
            let schemas = match self {
                TestExtractor::TextPlain => ExtractorSchema {
                    input_mimes: vec![mime::TEXT_PLAIN.to_string()],
                    ..Default::default()
                },
                TestExtractor::Wildcard => ExtractorSchema {
                    input_mimes: vec![mime::STAR_STAR.to_string()],
                    ..Default::default()
                },
            };
            Ok(schemas)
        }

        fn extract(
            &self,
            content: Vec<internal_api::Content>,
            _input_params: serde_json::Value,
        ) -> Result<Vec<Vec<internal_api::Content>>, anyhow::Error> {
            Ok(vec![content])
        }
    }

    #[test]
    fn test_matches_mime_type() {
        let mimetype_matcher = |extractor: TestExtractor, content_mimetypes: Vec<(&str, bool)>| {
            for (content_mime, expected) in content_mimetypes {
                let mut content = internal_api::ContentMetadata::default();
                content.content_type = content_mime.to_string();
                let matches = matches_mime_type(
                    &extractor.schemas().unwrap().input_mimes,
                    &content.content_type,
                );
                assert_eq!(
                    matches, expected,
                    "content mime type {} did not match for case {:?}",
                    content_mime, extractor
                );
            }
        };

        mimetype_matcher(
            TestExtractor::TextPlain,
            vec![
                (&mime::TEXT_PLAIN.to_string(), true),
                (&mime::IMAGE_PNG.to_string(), false),
                (&mime::APPLICATION_PDF.to_string(), false),
            ],
        );
        mimetype_matcher(
            TestExtractor::Wildcard,
            vec![
                (&mime::TEXT_PLAIN.to_string(), true),
                (&mime::IMAGE_PNG.to_string(), true),
                (&mime::APPLICATION_PDF.to_string(), true),
            ],
        );
    }
}
