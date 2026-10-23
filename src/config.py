"""
Module: config.py
Description: Configuration loader and schema container for the Credit Risk Data Pipeline.
             Supports local execution, Hive Metastore, and Azure Databricks Unity Catalog.
"""

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class BusinessRulesConfig:
    valid_grades: List[str] = field(default_factory=lambda: ["A", "B", "C", "D", "E", "F", "G"])
    min_loan_amount: float = 500.0
    max_loan_amount: float = 100000.0
    min_annual_income: float = 1000.0
    max_annual_income: float = 10000000.0
    min_interest_rate_pct: float = 0.0
    max_interest_rate_pct: float = 100.0
    min_state_sample_size: int = 10
    default_statuses: List[str] = field(default_factory=lambda: ["Charged Off", "Default"])
    resolved_statuses: List[str] = field(default_factory=lambda: ["Fully Paid", "Charged Off", "Default"])
    active_statuses: List[str] = field(
        default_factory=lambda: ["Current", "In Grace Period", "Late (16-30 days)", "Late (31-120 days)"]
    )
    income_bands: List[Dict[str, Any]] = field(
        default_factory=lambda: [
            {"min": 0, "max": 30000, "label": "1. Low (<$30k)", "sort_order": 1},
            {"min": 30000, "max": 60000, "label": "2. Lower-Middle ($30k-$60k)", "sort_order": 2},
            {"min": 60000, "max": 100000, "label": "3. Upper-Middle ($60k-$100k)", "sort_order": 3},
            {"min": 100000, "max": 150000, "label": "4. High ($100k-$150k)", "sort_order": 4},
            {"min": 150000, "max": 999999999, "label": "5. Very High (>$150k)", "sort_order": 5},
        ]
    )


@dataclass
class StorageConfig:
    raw_data_path: str = "data/raw/loan_data_sample.csv"
    delta_root_path: str = "data/delta"
    bronze_path: str = "data/delta/bronze/loans"
    silver_accepted_path: str = "data/delta/silver/loans"
    silver_quarantine_path: str = "data/delta/silver/loans_quarantine"
    gold_grade_path: str = "data/delta/gold/default_rate_by_grade"
    gold_state_path: str = "data/delta/gold/default_rate_by_state"
    gold_income_band_path: str = "data/delta/gold/default_rate_by_income_band"
    gold_portfolio_summary_path: str = "data/delta/gold/loan_portfolio_summary"
    gold_window_analytics_path: str = "data/delta/gold/loan_window_analytics"
    dq_results_path: str = "data/delta/audit/dq_validation_results"


@dataclass
class DataQualityConfig:
    max_critical_null_pct: float = 0.0
    max_warning_null_pct: float = 20.0
    min_expected_default_rate_pct: float = 1.0
    max_expected_default_rate_pct: float = 60.0


@dataclass
class PipelineConfig:
    environment: str = "local"
    storage: StorageConfig = field(default_factory=StorageConfig)
    business_rules: BusinessRulesConfig = field(default_factory=BusinessRulesConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)
    unity_catalog: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, file_path: str) -> "PipelineConfig":
        """Load configuration from a JSON file path."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        storage_data = data.get("storage", {})
        rules_data = data.get("business_rules", {})
        dq_data = data.get("data_quality", {})
        uc_data = data.get("unity_catalog", {})

        return cls(
            environment=data.get("environment", "local"),
            storage=StorageConfig(**storage_data),
            business_rules=BusinessRulesConfig(**rules_data),
            data_quality=DataQualityConfig(**dq_data),
            unity_catalog=uc_data,
        )

    def get_table_target(self, table_alias: str) -> str:
        """
        Returns the appropriate target table path or Unity Catalog reference
        based on the environment and UC configuration.
        """
        if self.unity_catalog.get("enabled", False):
            uc_tables = self.unity_catalog.get("tables", {})
            if table_alias in uc_tables:
                return uc_tables[table_alias]

        # Local or path-based Delta fallback
        path_attr = f"{table_alias}_path"
        if hasattr(self.storage, path_attr):
            return getattr(self.storage, path_attr)

        mapping = {
            "bronze_loans": self.storage.bronze_path,
            "silver_loans": self.storage.silver_accepted_path,
            "silver_quarantine": self.storage.silver_quarantine_path,
            "gold_default_by_grade": self.storage.gold_grade_path,
            "gold_default_by_state": self.storage.gold_state_path,
            "gold_default_by_income_band": self.storage.gold_income_band_path,
            "gold_portfolio_summary": self.storage.gold_portfolio_summary_path,
            "gold_window_analytics": self.storage.gold_window_analytics_path,
            "dq_validation_results": self.storage.dq_results_path,
        }
        return mapping.get(table_alias, getattr(self.storage, "delta_root_path", "data/delta"))
