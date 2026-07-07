"""Deterministic pre-deploy invariant proofs (WI-19 §4.1).

`inv_det.py` is the pre-flight gate: it runs the checks that must be green before any dynamic
staging run, with no live S3/cluster. See stress-test/plan.md §4.1.
"""
