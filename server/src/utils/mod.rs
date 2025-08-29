use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use anyhow::{anyhow, Result};
use futures::Stream;
use pin_project::{pin_project, pinned_drop};

use crate::data_model::EpochTime;

pub mod dynamic_sleep;

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
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH")
        .as_millis() as u64
}

pub enum TimeUnit {
    Milliseconds,
    Nanoseconds,
}

pub fn get_elapsed_time(at: &EpochTime, unit: TimeUnit) -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            // Convert both times to the same unit (nanoseconds) for comparison
            let current_ns = duration.as_nanos();
            let at_ns = at.as_nanos();

            let at_ns = match unit {
                TimeUnit::Milliseconds => at_ns * 1_000_000,
                TimeUnit::Nanoseconds => at_ns,
            };

            if current_ns < at_ns {
                return 0.0;
            }

            // Calculate difference and convert to seconds
            ((current_ns - at_ns) as f64) / 1_000_000_000.0
        }
        Err(_) => 0.0,
    }
}

#[cfg(test)]
pub fn json_to_cbor(value: serde_json::Value) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    ciborium::ser::into_writer(&value, &mut buf)
        .map_err(|e| anyhow!("unable to convert to cbor {e}"))?;
    Ok(buf)
}

#[cfg(test)]
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

    #[test]
    pub fn test_json_to_cbor() {
        let json_str = r#"{"key": "value"}"#;
        let result = text_to_cbor(json_str).unwrap();
        assert!(!result.is_empty());
        let data = ciborium::de::from_reader::<serde_json::Value, _>(&*result).unwrap();
        assert_eq!(data["key"], "value");
    }

    // adding a buffer of 100ms to prevent flakiness
    #[test]
    fn test_get_elapsed_time() {
        {
            let now = get_epoch_time_in_ms();
            let elapsed = get_elapsed_time(&now.into(), TimeUnit::Milliseconds);
            assert!((0.0..0.1).contains(&elapsed));
        }

        {
            let now = get_epoch_time_in_ms();
            let past = now - 10; // 10ms ago
            let elapsed = get_elapsed_time(&past.into(), TimeUnit::Milliseconds);
            assert!((0.01..0.21).contains(&elapsed), "{}", elapsed);
        }

        {
            let now = get_epoch_time_in_ms();
            let past = now - 5000; // 5 seconds ago
            let elapsed = get_elapsed_time(&past.into(), TimeUnit::Milliseconds);
            assert!((5.0..5.1).contains(&elapsed));
        }

        {
            let now = get_epoch_time_in_ms();
            let future = now + 5000; // 5 seconds in the future
            let elapsed = get_elapsed_time(&future.into(), TimeUnit::Milliseconds);
            assert_eq!(elapsed, 0.0); // Should handle future times gracefully
        }
    }
}
