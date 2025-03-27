use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    sync::watch,
    time::{sleep_until, Instant, Sleep},
};

pub struct DynamicSleepFuture {
    receiver: watch::Receiver<Instant>,
    current_sleep: Pin<Box<Sleep>>,
    chunk_duration: Duration,
    final_target: Instant,
    min_sleep: Option<Duration>,
}

impl DynamicSleepFuture {
    pub fn new(
        initial_time: Instant,
        chunk_duration: Duration,
        min_sleep: Option<Duration>,
    ) -> (Self, watch::Sender<Instant>) {
        let (sender, receiver) = watch::channel(initial_time);
        (
            Self {
                receiver,
                current_sleep: Box::pin(sleep_until(initial_time)),
                chunk_duration,
                final_target: initial_time, // Initialize final target
                min_sleep,
            },
            sender,
        )
    }
}

impl Future for DynamicSleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 1. Check if the watch channel has a new value
        if self.receiver.has_changed().unwrap_or(false) {
            let new_target = *self.receiver.borrow_and_update();

            // Update the final target to the new value
            self.final_target = new_target;
        }

        let now = Instant::now();

        // 2. If we've reached or passed the final target, complete the future
        if now >= self.final_target {
            return Poll::Ready(());
        }

        // 3. Calculate the next sleep time, either a chunk duration from now
        // or up to the final target, whichever is shorter
        let next_sleep_time = std::cmp::min(
            now + self.chunk_duration,
            // If min_sleep is set, use it to determine the next sleep time
            std::cmp::max(self.final_target, now + self.min_sleep.unwrap_or_default()),
        );

        // 4. Update current sleep if the deadline has changed
        if self.current_sleep.as_mut().as_ref().deadline() != next_sleep_time {
            self.current_sleep = Box::pin(sleep_until(next_sleep_time));
        }

        // 5. Poll the current sleep future
        match self.current_sleep.as_mut().poll(cx) {
            Poll::Ready(()) => {
                // 6. If we haven't reached final target, prepare current sleep for the next
                //    poll
                if now < self.final_target {
                    let next_next_sleep_time =
                        std::cmp::min(now + self.chunk_duration, self.final_target);
                    self.current_sleep = Box::pin(sleep_until(next_next_sleep_time));
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration as StdDuration;

    use tokio::time::{Duration, Instant};

    use super::*;

    #[tokio::test]
    async fn test_initial_sleep() {
        let now = Instant::now();
        let target_time = now + Duration::from_millis(100);
        let (sleep_future, _sender) =
            DynamicSleepFuture::new(target_time, Duration::from_millis(50), None);

        // Await the future
        tokio::time::timeout(Duration::from_millis(500), sleep_future)
            .await
            .unwrap();

        // Verify that we've slept approximately the right amount of time
        let elapsed = now.elapsed();
        assert!(elapsed >= StdDuration::from_millis(100));
        assert!(elapsed < StdDuration::from_millis(200)); // Allow some buffer
    }

    #[tokio::test]
    async fn test_dynamic_update_before_sleep_completes() {
        let now = Instant::now();
        let initial_target = now + Duration::from_millis(200);
        let (sleep_future, sender) =
            DynamicSleepFuture::new(initial_target, Duration::from_millis(50), None);

        let sender_clone = sender.clone();
        // Wait a bit, then update the target
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let new_target = now + Duration::from_millis(300);
            sender_clone.send(new_target).unwrap();
        });

        // Await the future
        tokio::time::timeout(Duration::from_millis(500), sleep_future)
            .await
            .unwrap();

        // Verify that we've slept approximately the updated duration
        let elapsed = now.elapsed();
        assert!(elapsed >= StdDuration::from_millis(300));
        assert!(elapsed < StdDuration::from_millis(400)); // Allow some buffer
    }

    #[tokio::test]
    async fn test_multiple_dynamic_updates() {
        let now = Instant::now();
        let initial_target = now + Duration::from_millis(400);
        let (sleep_future, sender) =
            DynamicSleepFuture::new(initial_target, Duration::from_millis(50), None);

        // Multiple updates of the target time
        let updates = vec![
            (50, 300),  // First update after 50ms to 300ms
            (100, 350), // Second update after 100ms to 350ms
            (150, 400), // Final update after 150ms to 400ms
        ];
        let sender_clone = sender.clone();

        // Spawn a task to make dynamic updates
        tokio::spawn(async move {
            for (delay_ms, new_duration_ms) in updates {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                let new_target = now + Duration::from_millis(new_duration_ms);
                sender_clone.send(new_target).unwrap();
            }
        });

        // Await the future
        tokio::time::timeout(Duration::from_millis(500), sleep_future)
            .await
            .unwrap();

        // Verify that we've slept approximately the final duration
        let elapsed = now.elapsed();
        assert!(elapsed >= StdDuration::from_millis(400));
        assert!(elapsed < StdDuration::from_millis(500)); // Allow some buffer
    }

    #[tokio::test]
    async fn test_update_to_shorter_duration() {
        let now = Instant::now();
        let initial_target = now + Duration::from_millis(300);
        let (sleep_future, sender) =
            DynamicSleepFuture::new(initial_target, Duration::from_millis(50), None);

        let sender_clone = sender.clone();
        // Update to a shorter duration after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let new_target = now + Duration::from_millis(100);
            sender_clone.send(new_target).unwrap();
        });

        // Await the future
        tokio::time::timeout(Duration::from_millis(500), sleep_future)
            .await
            .unwrap();

        // Verify that we've slept approximately the updated (shorter) duration
        let elapsed = now.elapsed();
        assert!(elapsed >= StdDuration::from_millis(100));
        assert!(elapsed < StdDuration::from_millis(200)); // Allow some buffer
    }

    #[tokio::test]
    async fn test_update_to_longer_duration() {
        let now = Instant::now();
        let initial_target = now + Duration::from_millis(150);
        let (sleep_future, sender) =
            DynamicSleepFuture::new(initial_target, Duration::from_millis(50), None);

        let sender_clone = sender.clone();

        // Update to a longer duration after a more substantial delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(52)).await;
            let new_target = now + Duration::from_millis(300);
            sender_clone.send(new_target).unwrap();
        });

        // Await the future
        tokio::time::timeout(Duration::from_millis(500), sleep_future)
            .await
            .unwrap();

        // Verify that we've slept approximately the updated (longer) duration
        let elapsed = now.elapsed();
        assert!(
            elapsed >= StdDuration::from_millis(300),
            "Elapsed time: {:?}",
            elapsed
        );
        assert!(
            elapsed < StdDuration::from_millis(400),
            "Elapsed time: {:?}",
            elapsed
        ); // Allow some buffer
    }
}
