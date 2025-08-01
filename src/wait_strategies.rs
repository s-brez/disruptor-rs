//! Module with different strategies for waiting for an event to be published.
//!
//! The lowest latency possible is the [`BusySpin`] strategy.
//!
//! To "waist" less CPU time and power, use one of the other strategies which have higher latency.

use std::{hint, time::{Duration, Instant}};

use crate::Sequence;

/// Wait strategies are used by consumers when no new events are ready on the ring buffer.
pub trait WaitStrategy: Send {
	/// The wait strategy will wait for the sequence id being available.
	fn wait_for(&mut self, sequence: Sequence);
}

/// Busy spin wait strategy. Lowest possible latency.
#[derive(Copy, Clone)]
pub struct BusySpin;

impl WaitStrategy for BusySpin {
	#[inline]
	fn wait_for(&mut self, _sequence: Sequence) {
		// Do nothing, true busy spin.
	}
}

/// Busy spin wait strategy with spin loop hint which enables the processor to optimize its behavior
/// by e.g. saving power os switching hyper threads. Obviously, this can induce latency.
///
/// See also [`BusySpin`].
#[derive(Copy, Clone)]
pub struct BusySpinWithSpinLoopHint;

impl WaitStrategy for BusySpinWithSpinLoopHint {
	fn wait_for(&mut self, _sequence: Sequence) {
		hint::spin_loop();
	}
}

/// Wait strategy that "backs off" from spinning to yielding and finally to a short park.
///
/// Spins while `counter` > `SPIN_THRESHOLD`, yields while `counter` > 0, and parks the thread 
/// for `sleep_time` once the counter reaches 0.
///
/// `park_timeout()` keeps scheduler latency low (futex fast path onLinux).
#[derive(Clone)]
pub struct SleepingWaitStrategy {
    retries: i32,
    sleep_time: Duration,
    counter: i32,
    last_sequence: Sequence,
}

// Default configuration values taken from the upstream Java implementation.
const DEFAULT_RETRIES: i32 = 200;
const SPIN_THRESHOLD: i32 = 100;
const DEFAULT_SLEEP_NANOS: u64 = 100;

impl Default for SleepingWaitStrategy {
    fn default() -> Self {
        Self::new(DEFAULT_RETRIES, Duration::from_nanos(DEFAULT_SLEEP_NANOS))
    }
}

impl SleepingWaitStrategy {
    /// Create a new strategy with a custom number of `retries`. Uses default sleep duration of 100 ns.
    pub fn with_retries(retries: i32) -> Self {
        Self::new(retries, Duration::from_nanos(DEFAULT_SLEEP_NANOS))
    }

    /// Create a new strategy with custom `retries` and `sleep_time`.
    pub fn new(retries: i32, sleep_time: Duration) -> Self {
        Self {
            retries,
            sleep_time,
            counter: retries,
            last_sequence: -1,
        }
    }

    #[inline]
    fn apply_wait_method(&mut self) {
        if self.counter > SPIN_THRESHOLD {
            // Spin
            self.counter -= 1;
            hint::spin_loop();
        } else if self.counter > 0 {
            // Yield
            self.counter -= 1;
            std::thread::yield_now();
        } else {
            // Park
            std::thread::park_timeout(self.sleep_time);
        }
    }
}

impl WaitStrategy for SleepingWaitStrategy {
    fn wait_for(&mut self, sequence: Sequence) {
        // Reset state machine when waiting for a different sequence.
        if sequence != self.last_sequence {
            self.counter = self.retries;
            self.last_sequence = sequence;
        }

        self.apply_wait_method();
    }
}

/// 3-phase back-off: spin -> yield -> delegate to fallback strategy.
#[derive(Clone)]
pub struct PhasedBackoff<F>
where
    F: WaitStrategy + Clone,
{
    spin_tries: i32,
    spin_timeout: Duration,
    yield_timeout: Duration,
    fallback: F,
    counter: i32,
    start_time: Option<Instant>,
    last_sequence: Sequence,
}

impl<F> PhasedBackoff<F>
where
    F: WaitStrategy + Clone,
{
    /// `spin_timeout`   Duration spent in the busy-spin phase before the thread starts yielding.
    /// `yield_timeout`  Extra time spent yielding before delegation to `fallback` strategy.
    /// `fallback`       Wait strategy used after the back-off time budget has been exhausted.
    pub fn new(spin_timeout: Duration, yield_timeout: Duration, fallback: F) -> Self {
        Self {
            spin_tries: 10_000,
            spin_timeout,
            yield_timeout: spin_timeout + yield_timeout,
            fallback,
            counter: 10_000,
            start_time: None,
            last_sequence: -1,
        }
    }

}

impl PhasedBackoff<SleepingWaitStrategy> {
    /// Helper to create a `PhasedBackoff` that delegates to `SleepingWaitStrategy` once its
    /// spin/yield budget is exhausted.
    pub fn with_sleep(spin_timeout: Duration, yield_timeout: Duration) -> Self {
        Self::new(spin_timeout, yield_timeout, SleepingWaitStrategy::default())
    }
}

impl<F> WaitStrategy for PhasedBackoff<F>
where
    F: WaitStrategy + Clone,
{
    fn wait_for(&mut self, sequence: Sequence) {
        
        // Reset state machine if sequence advanced.
        if sequence != self.last_sequence {
            self.counter = self.spin_tries;
            self.start_time = None;
            self.last_sequence = sequence;
        }

        // Spin phase
        self.counter -= 1;
        if self.counter > 0 {
            hint::spin_loop();
            return;
        }

        let now = Instant::now();
        let start = self.start_time.get_or_insert(now);
        let elapsed = now.duration_since(*start);

        if elapsed > self.yield_timeout {
            // Delegate to fallback strategy.
            self.fallback.wait_for(sequence);
        } else if elapsed > self.spin_timeout {
            // Yield
            std::thread::yield_now();
        } else {
            hint::spin_loop();
        }

        self.counter = self.spin_tries;
    }
}
