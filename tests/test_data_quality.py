"""
Module: test_data_quality.py
Description: Unit tests for Data Quality evaluators and validation logic using pytest.
             Tests both pure Python evaluators and threshold boundary conditions.
"""

import pytest
from src.data_quality import (
    DataQualityResult,
    evaluate_categorical_domain,
    evaluate_default_rate_sanity,
    evaluate_null_percentage,
    evaluate_numeric_range,
    evaluate_row_count_reconciliation,
    evaluate_uniqueness,
)


class TestNullPercentageEvaluator:
    def test_null_percentage_pass(self):
        status, pct, details = evaluate_null_percentage(null_count=0, total_count=1000, max_allowed_pct=0.0)
        assert status == "PASS"
        assert pct == 0.0

    def test_null_percentage_fail(self):
        status, pct, details = evaluate_null_percentage(null_count=50, total_count=1000, max_allowed_pct=1.0, severity="CRITICAL")
        assert status == "FAIL"
        assert pct == 5.0
        assert "exceeds" in details

    def test_null_percentage_warning(self):
        status, pct, details = evaluate_null_percentage(null_count=15, total_count=1000, max_allowed_pct=1.0, severity="WARNING")
        assert status == "WARN"
        assert pct == 1.5

    def test_null_percentage_zero_total(self):
        status, pct, details = evaluate_null_percentage(null_count=0, total_count=0, max_allowed_pct=0.0)
        assert status == "PASS"
        assert pct == 0.0


class TestUniquenessEvaluator:
    def test_uniqueness_pass(self):
        status, dup_count, details = evaluate_uniqueness(total_count=500, distinct_count=500)
        assert status == "PASS"
        assert dup_count == 0

    def test_uniqueness_fail(self):
        status, dup_count, details = evaluate_uniqueness(total_count=500, distinct_count=490)
        assert status == "FAIL"
        assert dup_count == 10
        assert "Found 10 duplicate keys" in details


class TestNumericRangeEvaluator:
    def test_range_pass(self):
        status, details = evaluate_numeric_range(
            observed_min=1000.0, observed_max=35000.0, allowed_min=500.0, allowed_max=100000.0
        )
        assert status == "PASS"
        assert "within permitted" in details

    def test_range_fail_below_min(self):
        status, details = evaluate_numeric_range(
            observed_min=-100.0, observed_max=35000.0, allowed_min=500.0, allowed_max=100000.0
        )
        assert status == "FAIL"
        assert "outside permitted" in details

    def test_range_fail_above_max(self):
        status, details = evaluate_numeric_range(
            observed_min=1000.0, observed_max=150000.0, allowed_min=500.0, allowed_max=100000.0
        )
        assert status == "FAIL"
        assert "outside permitted" in details


class TestCategoricalDomainEvaluator:
    def test_domain_pass(self):
        observed = {"A", "B", "C", "D"}
        allowed = {"A", "B", "C", "D", "E", "F", "G"}
        status, invalid, details = evaluate_categorical_domain(observed, allowed)
        assert status == "PASS"
        assert len(invalid) == 0

    def test_domain_fail(self):
        observed = {"A", "B", "Z", "99"}
        allowed = {"A", "B", "C", "D", "E", "F", "G"}
        status, invalid, details = evaluate_categorical_domain(observed, allowed)
        assert status == "FAIL"
        assert invalid == {"Z", "99"}
        assert "Encountered unexpected values" in details


class TestReconciliationEvaluator:
    def test_reconciliation_exact_pass(self):
        status, var_pct, details = evaluate_row_count_reconciliation(source_count=1000, target_count=1000)
        assert status == "PASS"
        assert var_pct == 0.0

    def test_reconciliation_variance_fail(self):
        status, var_pct, details = evaluate_row_count_reconciliation(
            source_count=1000, target_count=950, max_variance_pct=0.0
        )
        assert status == "FAIL"
        assert var_pct == 5.0


class TestDefaultRateSanityEvaluator:
    def test_sanity_pass(self):
        status, details = evaluate_default_rate_sanity(observed_default_rate_pct=15.4, min_pct=1.0, max_pct=60.0)
        assert status == "PASS"

    def test_sanity_fail_unrealistic_high(self):
        status, details = evaluate_default_rate_sanity(observed_default_rate_pct=85.0, min_pct=1.0, max_pct=60.0)
        assert status == "FAIL"

    def test_sanity_fail_unrealistic_low(self):
        status, details = evaluate_default_rate_sanity(observed_default_rate_pct=0.1, min_pct=1.0, max_pct=60.0)
        assert status == "FAIL"
