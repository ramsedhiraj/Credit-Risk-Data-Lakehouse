"""
Module: test_transformations.py
Description: Unit tests for credit risk transformations and business logic rules.
"""

import pytest
from src.config import BusinessRulesConfig, PipelineConfig


def test_business_rules_default_values():
    rules = BusinessRulesConfig()
    assert "A" in rules.valid_grades
    assert "G" in rules.valid_grades
    assert rules.min_loan_amount == 500.0
    assert rules.max_loan_amount == 100000.0
    assert "Charged Off" in rules.default_statuses
    assert "Default" in rules.default_statuses
    assert "Fully Paid" in rules.resolved_statuses


def test_income_band_classification():
    def get_income_band(income: float) -> str:
        if income < 30000:
            return "1. Low (<$30k)"
        elif income < 60000:
            return "2. Lower-Middle ($30k-$60k)"
        elif income < 100000:
            return "3. Upper-Middle ($60k-$100k)"
        elif income < 150000:
            return "4. High ($100k-$150k)"
        else:
            return "5. Very High (>$150k)"

    assert get_income_band(25000) == "1. Low (<$30k)"
    assert get_income_band(45000) == "2. Lower-Middle ($30k-$60k)"
    assert get_income_band(85000) == "3. Upper-Middle ($60k-$100k)"
    assert get_income_band(125000) == "4. High ($100k-$150k)"
    assert get_income_band(250000) == "5. Very High (>$150k)"


def test_credit_risk_default_logic():
    default_statuses = {"Charged Off", "Default"}
    resolved_statuses = {"Fully Paid", "Charged Off", "Default"}

    def is_default(status: str) -> int:
        return 1 if status in default_statuses else 0

    def is_resolved(status: str) -> int:
        return 1 if status in resolved_statuses else 0

    assert is_default("Charged Off") == 1
    assert is_default("Default") == 1
    assert is_default("Fully Paid") == 0
    assert is_default("Current") == 0
    assert is_default("Late (31-120 days)") == 0

    assert is_resolved("Charged Off") == 1
    assert is_resolved("Fully Paid") == 1
    assert is_resolved("Current") == 0
    assert is_resolved("In Grace Period") == 0


def test_config_loader():
    config = PipelineConfig.from_json("configs/project_config.example.json")
    assert config.environment == "local"
    assert config.storage.bronze_path == "data/delta/bronze/loans"
    assert config.business_rules.min_state_sample_size == 10
    assert config.get_table_target("bronze_loans") == "data/delta/bronze/loans"
