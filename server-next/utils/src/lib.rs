use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use futures::Stream;
use pin_project::{pin_project, pinned_drop};

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

pub trait OptionInspectNone<T> {
    fn inspect_none(self, inspector_function: impl FnOnce()) -> Self;
}

impl<T> OptionInspectNone<T> for Option<T> {
    fn inspect_none(self, inspector_function: impl FnOnce()) -> Self {
        match &self {
            Some(_) => (),
            None => inspector_function(),
        }
        self
    }
}

impl<T> OptionInspectNone<T> for &Option<T> {
    fn inspect_none(self, inspector_function: impl FnOnce()) -> Self {
        match &self {
            Some(_) => (),
            None => inspector_function(),
        }
        self
    }
}

pub fn get_epoch_time_in_ms() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH");
    since_the_epoch.as_millis() as u64
}

pub fn default_creation_time() -> SystemTime {
    UNIX_EPOCH
}

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

impl<S, F> StreamGuard<S, F>
where
    S: Stream,
    F: FnOnce(),
{
    /// Wraps the given [`Stream`], running the given closure upon being
    /// dropped.
    pub fn new(stream: S, on_drop: F) -> Self {
        Self {
            stream,
            on_drop: Some(on_drop),
        }
    }
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

/// A convenience extension for creating a [`StreamGuard`] via a method.
pub trait GuardStreamExt: Stream + Sized {
    /// Wraps the [`Stream`], running the given closure upon being dropped.
    fn guard<F>(self, on_drop: F) -> StreamGuard<Self, F>
    where
        F: FnOnce();
}

impl<S> GuardStreamExt for S
where
    S: Stream + Sized,
{
    fn guard<F>(self, on_drop: F) -> StreamGuard<Self, F>
    where
        F: FnOnce(),
    {
        StreamGuard::new(self, on_drop)
    }
}

#[cfg(test)]
pub mod tests {
    #[test]
    pub fn test_json_to_cbor() {
        let json_str = r#"{"key": "value"}"#;
        let result = super::text_to_cbor(json_str).unwrap();
        assert!(!result.is_empty());
        let data = ciborium::de::from_reader::<serde_json::Value, _>(&*result).unwrap();
        assert_eq!(data["key"], "value");
    }
}
