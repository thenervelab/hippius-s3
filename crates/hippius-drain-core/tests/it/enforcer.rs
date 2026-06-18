//! Decision table for the agent's admission valve (`Enforcer`).
//!
//! Every drain attempt passes three gates in order — circuit breaker, bandwidth
//! token bucket, concurrency limiter — and the first that refuses decides. This
//! one scenario walks a single enforcer through a stream of attempts crafted to
//! trip each gate in turn, tabulating the decision and the reason at every step.

use hippius_drain_core::{BreakerConfig, ByteRate, Bytes, CircuitBreaker, ConcurrencyLimiter, DenyReason, DrainDecision, Enforcer, TokenBucket};
use std::time::{Duration, Instant};

use crate::table::Table;

/// Renders a decision as the column text shown in the table.
fn label(decision: DrainDecision) -> String {
    match decision {
        DrainDecision::Allowed => "ALLOW".to_owned(),
        DrainDecision::Denied(DenyReason::BreakerOpen) => "DENY (breaker open)".to_owned(),
        DrainDecision::Denied(DenyReason::RateLimited) => "DENY (rate limited)".to_owned(),
        DrainDecision::Denied(DenyReason::AtConcurrencyLimit) => "DENY (at concurrency limit)".to_owned(),
    }
}

#[test]
fn admission_valve_decision_stream() {
    // Breaker trips after 2 consecutive failures; bucket refills 1000 B/s up to a
    // 1000 B burst; only 1 drain may be in flight at a time.
    let t0 = Instant::now();
    let mut enforcer = Enforcer::new(
        CircuitBreaker::new(BreakerConfig {
            failure_threshold: 2,
            cooldown: Duration::from_secs(5),
        }),
        TokenBucket::new(ByteRate::new(1_000), Bytes::new(1_000), t0),
        ConcurrencyLimiter::new(1),
    );

    let mut table = Table::new(
        "agent admission valve: gates in order  breaker -> rate -> concurrency",
        &["t", "request", "decision", "why"],
    );
    let record = |table: &mut Table, t: &str, req: &str, decision: DrainDecision, why: &str| {
        table.row(&[t.to_owned(), req.to_owned(), label(decision), why.to_owned()]);
    };

    // 1) Full burst and a free permit: the drain is admitted and now holds the
    //    single concurrency permit.
    let d1 = enforcer.try_drain(1_000, t0);
    record(&mut table, "+0s", "drain 1000 B", d1, "burst covers it; permit taken");
    assert_eq!(d1, DrainDecision::Allowed);

    // 2) Same instant: the burst is spent and no time has passed to refill, so
    //    the bandwidth gate sheds the attempt before concurrency is even checked.
    let d2 = enforcer.try_drain(1_000, t0);
    record(&mut table, "+0s", "drain 1000 B", d2, "tick budget already spent");
    assert_eq!(d2, DrainDecision::Denied(DenyReason::RateLimited));

    // 3) +1s: the bucket has refilled, but drain (1) still holds the only permit,
    //    so concurrency denies — and the tokens taken at the bandwidth gate are
    //    refunded, so a rejected drain costs no budget.
    let t1 = t0 + Duration::from_secs(1);
    let d3 = enforcer.try_drain(1_000, t1);
    record(&mut table, "+1s", "drain 1000 B", d3, "1 in flight, cap=1; tokens refunded");
    assert_eq!(d3, DrainDecision::Denied(DenyReason::AtConcurrencyLimit));

    // 4) Drain (1) completes successfully: it releases its permit and the success
    //    resets the breaker's failure run.
    enforcer.record_outcome(true, t1);

    // 5) +1s still: the permit is free and the refund in (3) left the budget
    //    intact, so the attempt is admitted — proof the denied drain burnt nothing.
    let d5 = enforcer.try_drain(1_000, t1);
    record(&mut table, "+1s", "drain 1000 B", d5, "permit free; refunded budget intact");
    assert_eq!(d5, DrainDecision::Allowed);

    // 6) Two consecutive failed Ceph writes trip the breaker (threshold = 2);
    //    once open it sheds drains fast, ahead of the bucket and the permit.
    enforcer.record_outcome(false, t1); // failure 1 (also releases drain (5)'s permit)
    enforcer.record_outcome(false, t1); // failure 2 -> breaker opens
    let t2 = t0 + Duration::from_secs(2);
    let d6 = enforcer.try_drain(1_000, t2);
    record(&mut table, "+2s", "drain 1000 B", d6, "breaker open after 2 failures (cooldown 5s)");
    assert_eq!(d6, DrainDecision::Denied(DenyReason::BreakerOpen));

    println!("\n{}", table.render());
}
