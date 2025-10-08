use std::time::Duration;
#[cfg(not(test))]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use mock_instant::global::{SystemTime, UNIX_EPOCH};

/// Get the elapsed system time since the Unix Epoch in Milliseconds
pub fn get_epoch_time_in_ms() -> u64 {
    get_epoch_time().as_millis() as u64
}

/// Get the elapsed system time since the Unix Epoch in Nanoseconds
pub fn get_epoch_time_in_ns() -> u128 {
    get_epoch_time().as_nanos()
}

pub fn get_epoch_time_in_secs() -> u64 {
    get_epoch_time().as_secs()
}

fn get_epoch_time() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH")
}

pub enum TimeUnit {
    Milliseconds,
    Nanoseconds,
}

pub fn get_elapsed_time(at: u128, unit: TimeUnit) -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            // Convert both times to the same unit (nanoseconds) for comparison
            let current_ns = duration.as_nanos();

            let at_ns = match unit {
                TimeUnit::Milliseconds => at * 1_000_000,
                TimeUnit::Nanoseconds => at,
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
mod tests {
    use mock_instant::global::MockClock;

    use super::*;

    // adding a buffer of 100ms to prevent flakiness
    #[test]
    fn test_get_elapsed_time() {
        {
            let now = get_epoch_time_in_ms();

            MockClock::advance_system_time(Duration::from_secs_f64(0.01));
            let elapsed = get_elapsed_time(now.into(), TimeUnit::Milliseconds);
            assert!((0.0..0.1).contains(&elapsed), "{}", elapsed);
        }

        {
            MockClock::set_system_time(Duration::ZERO);
            MockClock::advance_system_time(Duration::from_secs(100));

            let now = get_epoch_time_in_ms();
            let past = now - 10; // 10ms ago
            let elapsed = get_elapsed_time(past.into(), TimeUnit::Milliseconds);
            assert!((0.01..0.21).contains(&elapsed), "{}", elapsed);
        }

        {
            MockClock::set_system_time(Duration::ZERO);
            MockClock::advance_system_time(Duration::from_secs(100));

            let now = get_epoch_time_in_ms();
            let past = now - 5000; // 5 seconds ago
            let elapsed = get_elapsed_time(past.into(), TimeUnit::Milliseconds);
            assert!((5.0..5.1).contains(&elapsed));
        }

        {
            MockClock::set_system_time(Duration::ZERO);
            MockClock::advance_system_time(Duration::from_secs(100));

            let now = get_epoch_time_in_ms();
            let future = now + 5000; // 5 seconds in the future
            let elapsed = get_elapsed_time(future.into(), TimeUnit::Milliseconds);
            assert_eq!(elapsed, 0.0); // Should handle future times gracefully
        }
    }

    #[test]
    fn test_get_epoch_time_granularity() {
        MockClock::set_system_time(Duration::from_nanos(9999999999999999));

        // Getting the epoch in Nanoseconds preserves the system time's granularity.
        let epoch = get_epoch_time_in_ns();
        assert_eq!(9999999999999999, epoch);

        // Getting the epoch in Millisends truncates the time and looses granularity.
        let epoch = get_epoch_time_in_ms();
        assert_eq!(9999999999, epoch);
    }
}
