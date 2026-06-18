//! Decision-demo integration suite.
//!
//! These tests are characterization/demonstration harnesses over cephor's
//! already-built decision logic: each one feeds a scenario through a public
//! decision function and prints a human-readable **decision table** so a reader
//! can see *how* the call decided, then asserts the decision so the table can
//! never drift from the code silently. Run them with output visible:
//!
//! ```text
//! cargo test -p hippius-drain-core --test it -- --nocapture
//! ```
//!
//! One binary, suites as modules (axiom
//! `rust_quality_148_single_integration_test_binary`): one compile, one link,
//! shared fixtures (`table`) compiled once.
#![expect(
    clippy::print_stdout,
    reason = "the decision tables ARE the deliverable; they print to stdout for `cargo test -- --nocapture`"
)]
#![expect(clippy::unwrap_used, reason = "test code: id parsing and mutex locks unwrap by convention")]

mod table;

mod allocator;
mod enforcer;
