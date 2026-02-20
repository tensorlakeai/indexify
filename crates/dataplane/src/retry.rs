//! Generic retry utilities.
//!
//! Two flavors:
//! - [`retry_until_deadline`] — retries until a wall-clock deadline, with fixed
//!   polling interval and an optional abort check.
//! - [`retry_with_backoff`] — retries up to a fixed number of attempts, with
//!   configurable fixed or exponential backoff.

use std::time::Duration;

use anyhow::Result;
use tracing::{debug, warn};

// ---------------------------------------------------------------------------
// Backoff strategy
// ---------------------------------------------------------------------------

/// Backoff strategy for [`retry_with_backoff`].
#[allow(dead_code)]
pub enum Backoff {
    /// Fixed delay between retries.
    Fixed(Duration),
    /// Exponential backoff: delay doubles each retry, capped at `max`.
    #[allow(dead_code)] // Public API — used in tests, available for callers
    Exponential { initial: Duration, max: Duration },
}

impl Backoff {
    fn initial_delay(&self) -> Duration {
        match self {
            Backoff::Fixed(d) => *d,
            Backoff::Exponential { initial, .. } => *initial,
        }
    }

    fn next_delay(&self, current: Duration) -> Duration {
        match self {
            Backoff::Fixed(d) => *d,
            Backoff::Exponential { max, .. } => std::cmp::min(current * 2, *max),
        }
    }
}

// ---------------------------------------------------------------------------
// retry_with_backoff
// ---------------------------------------------------------------------------

/// Retry an operation up to `max_retries` times with configurable backoff.
///
/// Total attempts = 1 + `max_retries`. On each failure, `is_retryable` is
/// called: non-retryable errors are returned immediately without further
/// attempts.
#[allow(dead_code)]
pub async fn retry_with_backoff<T, E, F, Fut, R>(
    max_retries: u32,
    backoff: Backoff,
    description: &str,
    mut attempt_fn: F,
    mut is_retryable: R,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
    R: FnMut(&E) -> bool,
{
    let mut delay = backoff.initial_delay();

    for attempt in 0..=max_retries {
        match attempt_fn().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                if !is_retryable(&e) || attempt == max_retries {
                    return Err(e);
                }
                warn!(
                    attempt = attempt + 1,
                    max_retries,
                    delay_ms = delay.as_millis() as u64,
                    description,
                    error = %e,
                    "Attempt failed, retrying with backoff"
                );
                tokio::time::sleep(delay).await;
                delay = backoff.next_delay(delay);
            }
        }
    }

    unreachable!()
}

// ---------------------------------------------------------------------------
// retry_until_deadline
// ---------------------------------------------------------------------------

/// Retry an async operation until it succeeds or the deadline is reached.
///
/// Between retries, `abort_check` is called. If it returns `Err`, that error
/// is propagated immediately without further retries — useful for detecting
/// fatal conditions like process death.
///
/// For simple retries without abort checking, pass `|| async { Ok(()) }`.
pub async fn retry_until_deadline<T, F, Fut, A, AFut>(
    timeout: Duration,
    poll_interval: Duration,
    description: &str,
    mut attempt: F,
    mut abort_check: A,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
    A: FnMut() -> AFut,
    AFut: std::future::Future<Output = Result<()>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_err = None;

    loop {
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "Timeout {} after {:?}: {}",
                description,
                timeout,
                last_err
                    .as_ref()
                    .map(|e: &anyhow::Error| e.to_string())
                    .unwrap_or_default()
            );
        }

        match attempt().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                debug!(error = %e, description = %description, "Attempt failed, retrying...");
                last_err = Some(e);
            }
        }

        abort_check().await?;

        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_succeeds_immediately() {
        let result: Result<i32> = retry_until_deadline(
            Duration::from_secs(1),
            Duration::from_millis(10),
            "test op",
            || async { Ok(42) },
            || async { Ok(()) },
        )
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_succeeds_after_retries() {
        let mut count = 0;
        let result: Result<i32> = retry_until_deadline(
            Duration::from_secs(1),
            Duration::from_millis(10),
            "test op",
            || {
                count += 1;
                let c = count;
                async move {
                    if c < 3 {
                        anyhow::bail!("not yet");
                    }
                    Ok(c)
                }
            },
            || async { Ok(()) },
        )
        .await;
        assert_eq!(result.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_times_out() {
        let result: Result<i32> = retry_until_deadline(
            Duration::from_millis(50),
            Duration::from_millis(10),
            "connecting to server",
            || async { anyhow::bail!("connection refused") },
            || async { Ok(()) },
        )
        .await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Timeout"));
        assert!(err.contains("connecting to server"));
    }

    #[tokio::test]
    async fn test_abort_check_stops_early() {
        let result: Result<i32> = retry_until_deadline(
            Duration::from_secs(10),
            Duration::from_millis(10),
            "test op",
            || async { anyhow::bail!("fail") },
            || async { anyhow::bail!("process died") },
        )
        .await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("process died"));
    }

    // -- retry_with_backoff tests --

    #[tokio::test]
    async fn test_backoff_succeeds_immediately() {
        let result: std::result::Result<i32, String> = retry_with_backoff(
            3,
            Backoff::Fixed(Duration::from_millis(10)),
            "test op",
            || async { Ok(42) },
            |_: &String| true,
        )
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_backoff_succeeds_after_retries() {
        let mut count = 0;
        let result: std::result::Result<i32, String> = retry_with_backoff(
            3,
            Backoff::Fixed(Duration::from_millis(10)),
            "test op",
            || {
                count += 1;
                let c = count;
                async move {
                    if c < 3 {
                        Err(format!("fail {}", c))
                    } else {
                        Ok(c)
                    }
                }
            },
            |_: &String| true,
        )
        .await;
        assert_eq!(result.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_backoff_exhausts_retries() {
        let result: std::result::Result<i32, String> = retry_with_backoff(
            2,
            Backoff::Fixed(Duration::from_millis(10)),
            "test op",
            || async { Err("always fails".to_string()) },
            |_: &String| true,
        )
        .await;
        assert_eq!(result.unwrap_err(), "always fails");
    }

    #[tokio::test]
    async fn test_backoff_non_retryable_stops_early() {
        let mut count = 0;
        let result: std::result::Result<i32, String> = retry_with_backoff(
            5,
            Backoff::Fixed(Duration::from_millis(10)),
            "test op",
            || {
                count += 1;
                let c = count;
                async move { Err(format!("error {}", c)) }
            },
            |e: &String| e != "error 2",
        )
        .await;
        // Should stop at attempt 2 (non-retryable), not exhaust all 5 retries
        assert_eq!(result.unwrap_err(), "error 2");
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_backoff_exponential() {
        let start = tokio::time::Instant::now();
        let result: std::result::Result<i32, String> = retry_with_backoff(
            2,
            Backoff::Exponential {
                initial: Duration::from_millis(50),
                max: Duration::from_millis(200),
            },
            "test op",
            || async { Err("fail".to_string()) },
            |_: &String| true,
        )
        .await;
        assert!(result.is_err());
        // 2 retries: 50ms + 100ms = 150ms minimum
        assert!(start.elapsed() >= Duration::from_millis(140));
    }
}
