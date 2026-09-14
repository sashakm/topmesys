use std::{
    hash::{BuildHasher, RandomState},
    time::Duration,
};

/// Describes how the delay between two delivery attempts grows.
#[derive(Debug, Clone, PartialEq)]
pub enum Backoff {
    /// Waits the same delay before every retry.
    Fixed(Duration),
    /// Waits `initial` before the first retry and adds `increment` before every further retry.
    Linear {
        initial: Duration,
        increment: Duration,
    },
    /// Waits `initial` before the first retry and multiplies the delay by `factor` before every
    /// further retry.
    Exponential { initial: Duration, factor: f64 },
}

/// Decides how often a failed delivery is retried and how long its subscription waits between
/// attempts. A delivery keeps its subscription's [concurrency](crate::Subscription::with_concurrency)
/// slot while it waits, so a subscription handling one message at a time retries it before moving
/// on to the next. Delays that would overflow saturate at [Duration::MAX], use
/// [with_max_delay](RetryPolicy::with_max_delay) to cap them.
/// #### Example
/// ```
/// use std::time::Duration;
/// use topmesys::RetryPolicy;
///
/// let policy = RetryPolicy::exponential(4, Duration::from_millis(100))
///     .with_max_delay(Duration::from_millis(500));
///
/// assert_eq!(policy.delay(1), Duration::from_millis(100));
/// assert_eq!(policy.delay(3), Duration::from_millis(400));
/// assert_eq!(policy.delay(4), Duration::from_millis(500));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    retries: u32,
    backoff: Backoff,
    max_delay: Option<Duration>,
    jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::none()
    }
}

impl RetryPolicy {
    /// Retries a failed delivery up to `retries` times, waiting according to `backoff`.
    pub fn new(retries: u32, backoff: Backoff) -> Self {
        Self {
            retries,
            backoff,
            max_delay: None,
            jitter: false,
        }
    }

    /// Never retries: a failed delivery is dead-lettered right away.
    pub fn none() -> Self {
        Self::new(0, Backoff::Fixed(Duration::ZERO))
    }

    pub fn fixed(retries: u32, delay: Duration) -> Self {
        Self::new(retries, Backoff::Fixed(delay))
    }

    pub fn linear(retries: u32, initial: Duration, increment: Duration) -> Self {
        Self::new(retries, Backoff::Linear { initial, increment })
    }

    /// Doubles the delay with every retry, starting at `initial`.
    pub fn exponential(retries: u32, initial: Duration) -> Self {
        Self::new(
            retries,
            Backoff::Exponential {
                initial,
                factor: 2.0,
            },
        )
    }

    /// Caps every delay at `max_delay`.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = Some(max_delay);
        self
    }

    /// Randomises every delay to between half and all of its computed value, so deliveries that
    /// failed together don't retry at the same moment.
    pub fn with_jitter(mut self) -> Self {
        self.jitter = true;
        self
    }

    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// The delay before the given retry, counting from `1`. Jitter is not applied.
    pub fn delay(&self, retry: u32) -> Duration {
        let step = retry.saturating_sub(1);
        let delay = match &self.backoff {
            Backoff::Fixed(delay) => *delay,
            Backoff::Linear { initial, increment } => increment
                .checked_mul(step)
                .and_then(|increment| initial.checked_add(increment))
                .unwrap_or(Duration::MAX),
            Backoff::Exponential { initial, factor } => {
                let exponent = i32::try_from(step).unwrap_or(i32::MAX);
                let nanos = (initial.as_nanos() as f64 * factor.powi(exponent)).round();
                if nanos >= u64::MAX as f64 {
                    Duration::MAX
                } else {
                    Duration::from_nanos(nanos as u64)
                }
            }
        };
        self.max_delay
            .map_or(delay, |max_delay| delay.min(max_delay))
    }

    pub(crate) fn jittered_delay(&self, retry: u32) -> Duration {
        let delay = self.delay(retry);
        if !self.jitter {
            return delay;
        }
        let half = delay / 2;
        let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        // Every RandomState is keyed differently, which is random enough to spread retries.
        let random = RandomState::new().hash_one(retry);
        half.saturating_add(Duration::from_nanos(random % spread.saturating_add(1)))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Backoff, RetryPolicy};

    #[test]
    fn test_backoff_delays() {
        let ms = Duration::from_millis;

        let fixed = RetryPolicy::fixed(3, ms(50));
        assert_eq!([1, 2, 3].map(|retry| fixed.delay(retry)), [ms(50); 3]);

        let linear = RetryPolicy::linear(3, ms(100), ms(50));
        assert_eq!(
            [1, 2, 3].map(|retry| linear.delay(retry)),
            [ms(100), ms(150), ms(200)]
        );

        let exponential = RetryPolicy::exponential(4, ms(100));
        assert_eq!(
            [1, 2, 3, 4].map(|retry| exponential.delay(retry)),
            [ms(100), ms(200), ms(400), ms(800)]
        );

        let custom = RetryPolicy::new(
            2,
            Backoff::Exponential {
                initial: ms(100),
                factor: 1.5,
            },
        );
        assert_eq!(custom.delay(3), ms(225));
        assert_eq!(RetryPolicy::none().retries(), 0);
    }

    #[test]
    fn test_delays_are_capped_and_saturate() {
        let capped = RetryPolicy::exponential(10, Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(5));
        assert_eq!(capped.delay(3), Duration::from_secs(4));
        assert_eq!(capped.delay(4), Duration::from_secs(5));

        assert_eq!(
            RetryPolicy::exponential(u32::MAX, Duration::from_secs(1)).delay(u32::MAX),
            Duration::MAX
        );
        assert_eq!(
            RetryPolicy::linear(u32::MAX, Duration::from_secs(1), Duration::MAX).delay(3),
            Duration::MAX
        );
    }

    #[test]
    fn test_jitter_stays_within_bounds() {
        let delay = Duration::from_millis(100);
        let jittered = RetryPolicy::fixed(1, delay).with_jitter();
        for _ in 0..100 {
            let jittered_delay = jittered.jittered_delay(1);
            assert!(
                jittered_delay >= delay / 2 && jittered_delay <= delay,
                "{jittered_delay:?}"
            );
        }
        assert_eq!(RetryPolicy::fixed(1, delay).jittered_delay(1), delay);
    }
}
