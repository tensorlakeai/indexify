use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(test)]
use anyhow::{anyhow, Result};
use futures::Stream;
use pin_project::{pin_project, pinned_drop};

pub mod dynamic_sleep;
mod time;
pub use time::*;

#[macro_export]
macro_rules! unwrap_or_continue {
    ($opt: expr) => {
        match $opt {
            Some(v) => v,
            None => {
                continue;
            }
        }
    };
}

/// A [`Stream`] wrapper that automatically runs a custom action when dropped.
#[pin_project(PinnedDrop)]
pub struct StreamGuard<S, F>
where
    S: Stream,
    F: FnOnce(),
{
    #[pin]
    stream: S,
    on_drop: Option<F>,
}

impl<S, F> Stream for StreamGuard<S, F>
where
    S: Stream,
    F: FnOnce(),
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[pinned_drop]
impl<S, F> PinnedDrop for StreamGuard<S, F>
where
    S: Stream,
    F: FnOnce(),
{
    fn drop(mut self: Pin<&mut Self>) {
        self.project().on_drop.take().expect(
            "No on_drop function in StreamGuard, was drop called twice or constructed wrongly?",
        )()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn json_to_cbor(value: serde_json::Value) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&value, &mut buf)
            .map_err(|e| anyhow!("unable to convert to cbor {e}"))?;
        Ok(buf)
    }

    pub fn text_to_cbor(text: &str) -> Result<Vec<u8>> {
        let value: serde_json::Value = serde_json::from_str(text)?;
        json_to_cbor(value)
    }

    #[test]
    pub fn test_json_to_cbor() {
        let json_str = r#"{"key": "value"}"#;
        let result = text_to_cbor(json_str).unwrap();
        assert!(!result.is_empty());
        let data = ciborium::de::from_reader::<serde_json::Value, _>(&*result).unwrap();
        assert_eq!(data["key"], "value");
    }
}
