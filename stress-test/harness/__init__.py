"""hippius-s3 drain-stack production-readiness harness.

A runnable suite that asserts production readiness against a live S3 endpoint (default
s3-staging.hippius.com) plus, when cluster access is available, the staging drain internals
(Postgres cephor_replication_status + Prometheus drain_* metrics).

See stress-test/plan.md for the full build spec and stress-test/README.md to run it.
"""
