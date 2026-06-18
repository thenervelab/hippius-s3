//! The agent's local control valve: a three-gate decision (circuit breaker →
//! bandwidth token bucket → concurrency limit) plus the decay-to-floor function
//! the agent applies when its allocation goes stale.
//!
//! All state machines here are pure functions of their inputs and an injected
//! [`Instant`] (from [`crate::Clock`]), so they are deterministic and testable
//! with a manually-advanced clock — no wall time, no I/O.

use crate::units::{ByteRate, Bytes};
use std::time::{Duration, Instant};

/// Tuning for the circuit breaker.
#[derive(Debug, Clone, Copy)]
pub struct BreakerConfig {
    /// Consecutive failures that trip the breaker open.
    pub failure_threshold: u32,
    /// How long the breaker stays open before allowing a trial request.
    pub cooldown: Duration,
}

/// Circuit-breaker phase.
///
/// `Open` carries the instant after which a trial request is allowed, so the
/// "still open" state cannot exist without a deadline to leave it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Requests flow normally.
    Closed,
    /// Requests are rejected until `until`.
    Open {
        /// The instant a trial request becomes allowed.
        until: Instant,
    },
    /// Cooldown elapsed; the next admission is the single trial request, which
    /// consumes this state into [`BreakerState::Probing`].
    HalfOpen,
    /// The single trial is in flight: it was admitted but its outcome is not yet
    /// recorded, so further requests are denied until `record_success` (closes) or
    /// `record_failure` (reopens) resolves it — this is what bounds the half-open
    /// phase to exactly one probe rather than a stampede.
    Probing,
}

/// A circuit breaker over the agent's observed Ceph write outcomes.
#[derive(Debug, Clone, Copy)]
pub struct CircuitBreaker {
    state: BreakerState,
    consecutive_failures: u32,
    config: BreakerConfig,
}

impl CircuitBreaker {
    /// A closed breaker with the given config.
    #[must_use]
    pub fn new(config: BreakerConfig) -> Self {
        Self {
            state: BreakerState::Closed,
            consecutive_failures: 0,
            config,
        }
    }

    /// The current phase (after applying any time-based transition from `now`).
    #[must_use]
    pub fn state(&mut self, now: Instant) -> BreakerState {
        if let BreakerState::Open { until } = self.state
            && now >= until
        {
            self.state = BreakerState::HalfOpen;
        }
        self.state
    }

    /// Whether a request may proceed at `now`. `Open`→`HalfOpen` elapses here, and
    /// admitting the half-open trial consumes it into `Probing` so the breaker
    /// admits exactly one probe until the trial's outcome is recorded.
    #[must_use]
    pub fn allows(&mut self, now: Instant) -> bool {
        match self.state(now) {
            BreakerState::Open { .. } | BreakerState::Probing => false,
            BreakerState::Closed => true,
            BreakerState::HalfOpen => {
                // Consume the single trial: admit this one and move to Probing, so a
                // concurrent second request is denied until record_outcome resolves it.
                self.state = BreakerState::Probing;
                true
            }
        }
    }

    /// Records a successful Ceph write: the breaker closes and the failure run resets.
    pub fn record_success(&mut self) {
        self.state = BreakerState::Closed;
        self.consecutive_failures = 0;
    }

    /// Records a failed Ceph write at `now`, tripping the breaker if warranted.
    ///
    /// A failure of the trial probe (`HalfOpen`/`Probing`) reopens immediately; in
    /// `Closed`, the breaker opens once consecutive failures reach the threshold.
    pub fn record_failure(&mut self, now: Instant) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let trip = matches!(self.state, BreakerState::HalfOpen | BreakerState::Probing)
            || (matches!(self.state, BreakerState::Closed) && self.consecutive_failures >= self.config.failure_threshold);
        if trip {
            // `now + cooldown` would panic on `Instant` overflow — invisible to the
            // panic-deny lint, since the panic lives in std's `Add`. Saturate
            // instead: an overflow needs an `Instant` within `cooldown` of the
            // monotonic-clock ceiling (centuries of uptime), and `unwrap_or(now)`
            // degrades to an instantly-elapsing Open state rather than aborting.
            self.state = BreakerState::Open {
                until: now.checked_add(self.config.cooldown).unwrap_or(now),
            };
        }
    }
}

/// A bandwidth token bucket: refills at `rate` bytes/s up to `burst`.
#[derive(Debug, Clone, Copy)]
pub struct TokenBucket {
    rate: ByteRate,
    burst: Bytes,
    tokens: u64,
    last: Instant,
    /// Sub-token credit carried between refills: the `rate * elapsed_nanos`
    /// remainder (in `0..1_000_000_000`) that did not form a whole byte-token this
    /// tick. Carrying it makes a low-rate / high-frequency refill exact instead of
    /// systematically under-crediting (each tick would otherwise truncate to zero).
    remainder_nanos: u64,
}

impl TokenBucket {
    /// A bucket starting full (a full burst available) at `now`.
    #[must_use]
    pub fn new(rate: ByteRate, burst: Bytes, now: Instant) -> Self {
        Self {
            rate,
            burst,
            tokens: burst.get(),
            last: now,
            remainder_nanos: 0,
        }
    }

    /// Replaces the refill rate (the adapter from an [`crate::Allocation`] budget).
    pub fn set_rate(&mut self, rate: ByteRate) {
        self.rate = rate;
    }

    /// The current refill rate (the enforced bandwidth budget).
    #[must_use]
    pub fn rate(&self) -> ByteRate {
        self.rate
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last);
        // Exact integer refill: tokens = (rate * elapsed_nanos + carried remainder)
        // / 1e9, carrying the new remainder so no sub-token credit is lost across
        // many small refills (#24). u128 avoids overflow; the remainder is always
        // < 1e9, so it fits a u64.
        let numerator = u128::from(self.rate.get()) * elapsed.as_nanos() + u128::from(self.remainder_nanos);
        let added = u64::try_from(numerator / 1_000_000_000).unwrap_or(u64::MAX);
        self.remainder_nanos = u64::try_from(numerator % 1_000_000_000).unwrap_or(0);
        self.tokens = self.tokens.saturating_add(added).min(self.burst.get());
        self.last = now;
    }

    /// Tries to spend `bytes` tokens, refilling first. Returns whether granted.
    #[must_use]
    pub fn try_take(&mut self, bytes: u64, now: Instant) -> bool {
        self.refill(now);
        if self.tokens >= bytes {
            self.tokens -= bytes;
            true
        } else {
            false
        }
    }

    /// Returns `bytes` tokens to the bucket (used when a later gate denies).
    pub fn refund(&mut self, bytes: u64) {
        self.tokens = self.tokens.saturating_add(bytes).min(self.burst.get());
    }
}

/// A cap on the number of concurrent in-flight drains.
#[derive(Debug, Clone, Copy)]
pub struct ConcurrencyLimiter {
    max: u32,
    in_flight: u32,
}

impl ConcurrencyLimiter {
    /// A limiter permitting `max` concurrent drains.
    #[must_use]
    pub fn new(max: u32) -> Self {
        Self { max, in_flight: 0 }
    }

    /// Tries to take a permit. Returns whether one was available.
    #[must_use]
    pub fn try_acquire(&mut self) -> bool {
        if self.in_flight < self.max {
            self.in_flight += 1;
            true
        } else {
            false
        }
    }

    /// Returns a permit. Saturates at zero so an over-release cannot underflow.
    pub fn release(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    /// The number of in-flight drains.
    #[must_use]
    pub fn in_flight(self) -> u32 {
        self.in_flight
    }
}

/// Decays `base` toward `floor` by halving once per elapsed `half_life`.
///
/// Used when the allocator goes silent: a stale allocation must not let a node
/// keep hammering Ceph, so the effective rate halves each `half_life` until it
/// reaches `floor`. Integer right-shift keeps it deterministic; a zero
/// `half_life` means "no decay defined" and returns `base` unchanged.
#[must_use]
pub fn decay_rate(base: ByteRate, floor: ByteRate, age: Duration, half_life: Duration) -> ByteRate {
    if half_life.is_zero() {
        return base;
    }
    let halvings = age.as_nanos() / half_life.as_nanos();
    // Shift cannot reach 64 (would be UB for u64); cap it, by which point the
    // value is 0 anyway and the floor clamp takes over.
    let shift = u32::try_from(halvings).unwrap_or(u32::MAX).min(63);
    let decayed = base.get() >> shift;
    // Clamp into `[min(floor, base), base]`: the floor is a lower bound on the
    // decay of a large base, never a target a small base climbs UP to. The final
    // `.min(base)` keeps decay non-increasing when `floor > base` (#9) — otherwise
    // a node throttled below its floor would gain rate the instant the allocator
    // went silent, increasing load on a possibly-still-critical Ceph.
    ByteRate::new(decayed.max(floor.get()).min(base.get()))
}

/// Why a drain attempt was denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenyReason {
    /// The circuit breaker is open.
    BreakerOpen,
    /// The bandwidth budget for this tick is exhausted.
    RateLimited,
    /// The concurrency limit is reached.
    AtConcurrencyLimit,
}

/// The outcome of a drain admission check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainDecision {
    /// The drain may proceed; a concurrency permit was taken.
    Allowed,
    /// The drain was rejected for the given reason; no permit was taken.
    Denied(DenyReason),
}

/// The agent's combined admission valve.
#[derive(Debug, Clone, Copy)]
pub struct Enforcer {
    breaker: CircuitBreaker,
    bucket: TokenBucket,
    concurrency: ConcurrencyLimiter,
}

impl Enforcer {
    /// Builds an enforcer from its three gates.
    #[must_use]
    pub fn new(breaker: CircuitBreaker, bucket: TokenBucket, concurrency: ConcurrencyLimiter) -> Self {
        Self {
            breaker,
            bucket,
            concurrency,
        }
    }

    /// Applies a fresh allocation by setting the bandwidth refill rate.
    pub fn set_rate(&mut self, rate: ByteRate) {
        self.bucket.set_rate(rate);
    }

    /// The bandwidth budget currently enforced (the token-bucket refill rate).
    #[must_use]
    pub fn rate(&self) -> ByteRate {
        self.bucket.rate()
    }

    /// Admits a drain of `bytes` at `now` through all three gates in order:
    /// breaker → bandwidth → concurrency. Tokens spent at the bandwidth gate are
    /// refunded if the concurrency gate then denies, so a rejected drain costs
    /// no budget.
    #[must_use]
    pub fn try_drain(&mut self, bytes: u64, now: Instant) -> DrainDecision {
        if !self.breaker.allows(now) {
            return DrainDecision::Denied(DenyReason::BreakerOpen);
        }
        if !self.bucket.try_take(bytes, now) {
            return DrainDecision::Denied(DenyReason::RateLimited);
        }
        if !self.concurrency.try_acquire() {
            self.bucket.refund(bytes);
            return DrainDecision::Denied(DenyReason::AtConcurrencyLimit);
        }
        DrainDecision::Allowed
    }

    /// Records a completed drain's outcome and releases its concurrency permit.
    pub fn record_outcome(&mut self, success: bool, now: Instant) {
        if success {
            self.breaker.record_success();
        } else {
            self.breaker.record_failure(now);
        }
        self.concurrency.release();
    }

    /// Returns a held concurrency permit without touching the breaker — the RAII
    /// recovery path for a drain that unwound or was cancelled before
    /// [`record_outcome`](Self::record_outcome) ran. `release` saturates at zero,
    /// so an erroneous double release cannot underflow the in-flight count.
    pub fn release_permit(&mut self) {
        self.concurrency.release();
    }
}

#[cfg(test)]
mod tests {
    use super::{BreakerConfig, BreakerState, CircuitBreaker, ConcurrencyLimiter, DenyReason, DrainDecision, Enforcer, TokenBucket, decay_rate};
    use crate::units::{ByteRate, Bytes};
    use proptest::prelude::*;
    use std::time::{Duration, Instant};

    fn t0() -> Instant {
        Instant::now()
    }

    // --- TokenBucket ---

    #[test]
    fn bucket_denies_once_burst_is_spent() {
        let now = t0();
        let mut bucket = TokenBucket::new(ByteRate::new(1_000), Bytes::new(1_000), now);
        assert!(bucket.try_take(1_000, now), "full burst should cover the first take");
        assert!(!bucket.try_take(1, now), "no tokens remain at the same instant");
    }

    #[test]
    fn bucket_refills_at_rate_over_time() {
        let now = t0();
        let mut bucket = TokenBucket::new(ByteRate::new(1_000), Bytes::new(1_000), now);
        assert!(bucket.try_take(1_000, now));
        // 0.5s at 1000 B/s refills ~500 tokens.
        let later = now + Duration::from_millis(500);
        assert!(bucket.try_take(500, later), "should have refilled ~500 tokens");
        assert!(!bucket.try_take(1, later));
    }

    #[test]
    fn bucket_credits_fractional_tokens_across_small_refills() {
        // #24: at a low rate with frequent refills, each step's exact credit is a
        // fraction of a token. Truncating per step loses it; carrying the remainder
        // accumulates correctly. 3 B/s over ten 100ms steps = exactly 3 tokens.
        let now = t0();
        let mut bucket = TokenBucket::new(ByteRate::new(3), Bytes::new(1_000), now);
        assert!(bucket.try_take(1_000, now), "drain the initial burst to zero");
        let mut t = now;
        for _ in 0..10 {
            t += Duration::from_millis(100);
            let _ = bucket.try_take(0, t); // refill without spending
        }
        assert!(bucket.try_take(3, t), "ten 0.3-token steps accumulated to 3 tokens");
        assert!(!bucket.try_take(1, t), "but no more than the 3 it earned");
    }

    proptest! {
        /// Refilling in many small steps credits exactly `floor(rate * total / 1e9)`
        /// tokens — the remainder carry makes the sum-of-steps equal the single-shot
        /// refill, so no sub-token credit is lost (#24). The truncating version
        /// under-credited here; the shrinker would have found it.
        #[test]
        fn refill_credits_exactly_across_small_steps(rate in 1u64..1_000_000, steps in 1u64..200, step_ms in 1u64..100) {
            let now = t0();
            let burst = 2_000_000_000u64; // large enough not to cap for these inputs
            let mut bucket = TokenBucket::new(ByteRate::new(rate), Bytes::new(burst), now);
            prop_assert!(bucket.try_take(burst, now), "drain the initial burst to zero");
            let mut t = now;
            for _ in 0..steps {
                t += Duration::from_millis(step_ms);
                let _ = bucket.try_take(0, t); // refill without spending
            }
            let total_nanos = u128::from(steps * step_ms) * 1_000_000;
            let expected = u64::try_from(u128::from(rate) * total_nanos / 1_000_000_000).unwrap_or(u64::MAX);
            prop_assert!(bucket.try_take(expected, t), "credited at least the exact total {expected}");
            prop_assert!(!bucket.try_take(1, t), "credited no more than the exact total {expected}");
        }
    }

    #[test]
    fn bucket_caps_at_burst() {
        let now = t0();
        let mut bucket = TokenBucket::new(ByteRate::new(1_000), Bytes::new(1_000), now);
        assert!(bucket.try_take(1_000, now));
        // 10s of refill would be 10_000 tokens, but burst caps at 1_000.
        let later = now + Duration::from_secs(10);
        assert!(bucket.try_take(1_000, later));
        assert!(!bucket.try_take(1, later), "tokens cannot exceed burst");
    }

    // --- CircuitBreaker ---

    fn breaker() -> CircuitBreaker {
        CircuitBreaker::new(BreakerConfig {
            failure_threshold: 3,
            cooldown: Duration::from_secs(5),
        })
    }

    #[test]
    fn breaker_opens_after_threshold_failures() {
        let now = t0();
        let mut b = breaker();
        assert!(b.allows(now));
        for _ in 0..3 {
            b.record_failure(now);
        }
        assert!(!b.allows(now), "should be open after 3 consecutive failures");
    }

    #[test]
    fn half_open_admits_only_a_single_trial() {
        // M2: after cooldown the breaker admits exactly ONE trial; a second request
        // while that trial is in flight (no outcome recorded yet) is denied, so a
        // recovering Ceph is probed by one drain, not stampeded by the whole backlog.
        let now = t0();
        let mut b = breaker();
        for _ in 0..3 {
            b.record_failure(now);
        }
        let after = now + Duration::from_secs(5);
        assert!(b.allows(after), "the first trial after cooldown is admitted");
        assert!(!b.allows(after), "a second trial while the first is in flight is denied");
    }

    #[test]
    fn breaker_half_opens_after_cooldown_then_closes_on_success() {
        let now = t0();
        let mut b = breaker();
        for _ in 0..3 {
            b.record_failure(now);
        }
        assert!(!b.allows(now));
        let after = now + Duration::from_secs(5);
        // Cooldown elapsed -> half-open (ready to probe); the trial admission then
        // consumes it into Probing (in flight); a success closes the breaker.
        assert_eq!(b.state(after), BreakerState::HalfOpen);
        assert!(b.allows(after), "the trial request is admitted");
        assert_eq!(b.state(after), BreakerState::Probing, "the trial is now in flight");
        b.record_success();
        assert_eq!(b.state(after), BreakerState::Closed);
        assert!(b.allows(after));
    }

    #[test]
    fn breaker_reopens_when_trial_fails() {
        let now = t0();
        let mut b = breaker();
        for _ in 0..3 {
            b.record_failure(now);
        }
        let after = now + Duration::from_secs(5);
        assert!(b.allows(after)); // half-open trial
        b.record_failure(after); // trial fails
        assert!(!b.allows(after), "a failed trial reopens the breaker");
    }

    #[test]
    fn breaker_success_resets_failure_run() {
        let now = t0();
        let mut b = breaker();
        b.record_failure(now);
        b.record_failure(now);
        b.record_success();
        b.record_failure(now);
        b.record_failure(now);
        assert!(b.allows(now), "two failures after a reset must not trip the threshold of 3");
    }

    // --- decay_rate ---

    #[test]
    fn decay_halves_per_half_life_and_floors() {
        let base = ByteRate::new(1_000);
        let floor = ByteRate::new(100);
        let hl = Duration::from_secs(1);
        assert_eq!(decay_rate(base, floor, Duration::ZERO, hl).get(), 1_000);
        assert_eq!(decay_rate(base, floor, Duration::from_secs(1), hl).get(), 500);
        assert_eq!(decay_rate(base, floor, Duration::from_secs(2), hl).get(), 250);
        // 1000 >> 4 = 62, below the floor of 100, so it clamps.
        assert_eq!(decay_rate(base, floor, Duration::from_secs(4), hl).get(), 100);
    }

    #[test]
    fn decay_zero_half_life_is_identity() {
        let base = ByteRate::new(777);
        assert_eq!(decay_rate(base, ByteRate::ZERO, Duration::from_secs(10), Duration::ZERO).get(), 777);
    }

    #[test]
    fn decay_never_exceeds_base_even_when_floor_is_higher() {
        // #9: decay is a non-INCREASING function. When the floor is configured above
        // the (small) last budget, the result must NOT climb to the floor — that
        // would raise load on a possibly-still-critical Ceph the moment the
        // allocator goes silent, the exact opposite of the safety the decay provides.
        let base = ByteRate::new(100);
        let floor = ByteRate::new(1_000);
        let hl = Duration::from_secs(1);
        assert_eq!(decay_rate(base, floor, Duration::ZERO, hl).get(), 100, "result never exceeds base");
        assert_eq!(
            decay_rate(base, floor, Duration::from_secs(5), hl).get(),
            100,
            "still capped at base, not the higher floor"
        );
    }

    // --- ConcurrencyLimiter ---

    #[test]
    fn concurrency_caps_in_flight_and_releases() {
        let mut c = ConcurrencyLimiter::new(2);
        assert!(c.try_acquire());
        assert!(c.try_acquire());
        assert!(!c.try_acquire(), "third acquire over a limit of 2 is denied");
        c.release();
        assert!(c.try_acquire(), "a release frees a permit");
        assert_eq!(c.in_flight(), 2);
    }

    // --- Enforcer (three-gate) ---

    fn enforcer(now: Instant) -> Enforcer {
        Enforcer::new(
            CircuitBreaker::new(BreakerConfig {
                failure_threshold: 1,
                cooldown: Duration::from_secs(5),
            }),
            TokenBucket::new(ByteRate::new(1_000), Bytes::new(1_000), now),
            ConcurrencyLimiter::new(1),
        )
    }

    #[test]
    fn enforcer_breaker_gate_wins_first() {
        let now = t0();
        let mut e = enforcer(now);
        assert_eq!(e.try_drain(100, now), DrainDecision::Allowed); // permit taken
        e.record_outcome(false, now); // one failure trips threshold=1, releases permit
        assert_eq!(e.try_drain(100, now), DrainDecision::Denied(DenyReason::BreakerOpen));
    }

    #[test]
    fn enforcer_rate_gate_denies_when_empty() {
        let now = t0();
        let mut e = enforcer(now);
        assert_eq!(e.try_drain(1_000, now), DrainDecision::Allowed);
        e.record_outcome(true, now); // release permit so concurrency is not the blocker
        assert_eq!(e.try_drain(1, now), DrainDecision::Denied(DenyReason::RateLimited));
    }

    #[test]
    fn enforcer_concurrency_gate_denies_and_refunds_tokens() {
        let now = t0();
        let mut e = enforcer(now);
        assert_eq!(e.try_drain(100, now), DrainDecision::Allowed); // holds the only permit
        // Second drain passes breaker + bandwidth but the permit is taken.
        assert_eq!(e.try_drain(100, now), DrainDecision::Denied(DenyReason::AtConcurrencyLimit));
        // The refund means the bandwidth budget was not consumed by the denied drain:
        // after releasing the permit, a 900-byte drain still fits the original 1000 burst.
        e.record_outcome(true, now);
        assert_eq!(e.try_drain(900, now), DrainDecision::Allowed);
    }

    #[test]
    fn enforcer_set_rate_applies_allocation() {
        let now = t0();
        let mut e = enforcer(now);
        assert_eq!(e.try_drain(1_000, now), DrainDecision::Allowed);
        e.record_outcome(true, now);
        e.set_rate(ByteRate::new(2_000)); // a larger allocation refills faster
        let later = now + Duration::from_secs(1);
        assert_eq!(
            e.try_drain(1_000, later),
            DrainDecision::Allowed,
            "refilled to the burst cap at the new rate"
        );
    }

    proptest! {
        /// `decay_rate` never exceeds `base`, never increases with age, and is the
        /// identity at age zero. `floor` is unconstrained (it may exceed `base`):
        /// the result must still stay `<= base` (#9), and respect the floor only as
        /// a lower bound when the floor is itself `<= base`.
        #[test]
        fn decay_is_bounded_and_non_increasing(
            base in 0u64..=1_000_000_000,
            floor in 0u64..=1_000_000_000,
            age_ms in 0u64..100_000,
            hl_ms in 1u64..10_000,
        ) {
            let b = ByteRate::new(base);
            let f = ByteRate::new(floor);
            let hl = Duration::from_millis(hl_ms);
            let r1 = decay_rate(b, f, Duration::from_millis(age_ms), hl).get();
            prop_assert!(r1 <= base, "decay {r1} exceeded base {base}");
            if floor <= base {
                prop_assert!(r1 >= floor, "decay {r1} fell below floor {floor} (floor <= base)");
            }
            let r2 = decay_rate(b, f, Duration::from_millis(age_ms + 1_000), hl).get();
            prop_assert!(r2 <= r1, "decay rose with age: {r1} -> {r2}");
            prop_assert_eq!(decay_rate(b, f, Duration::ZERO, hl).get(), base);
        }
    }
}
