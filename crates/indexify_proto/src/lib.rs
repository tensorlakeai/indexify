use indexify_coordinator::{content_source, ContentSource, Empty};

#[rustfmt::skip]
pub mod indexify_coordinator;
#[rustfmt::skip]
pub mod indexify_raft;

impl From<String> for ContentSource {
    fn from(s: String) -> Self {
        if s == "ingestion" {
            Self {
                value: Some(content_source::Value::Ingestion(Empty {})),
            }
        } else if s == "" {
            Self {
                value: Some(content_source::Value::None(Empty {})),
            }
        } else {
            Self {
                value: Some(content_source::Value::Policy(s)),
            }
        }
    }
}
