"""
Module: data_quality.py
Description: Reusable Data Quality Framework for the Credit Risk Data Pipeline.
             Contains both pure Python validation evaluators (for fast unit testing)
             and PySpark execution methods for Medallion layer validation and Delta Lake logging.
"""

import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import PipelineConfig
from src.schemas import DATA_QUALITY_RESULT_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("data_quality")


# -------------------------------------------------------------------------
# Data Structures
# -------------------------------------------------------------------------
@dataclass
class DataQualityResult:
    check_name: str
    table_name: str
    column_name: Optional[str]
    check_type: str
    observed_value: str
    expected_value: str
    status: str       # "PASS", "WARN", "FAIL"
    severity: str     # "CRITICAL", "WARNING"
    details: str
    executed_at: datetime = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_name": self.check_name,
            "table_name": self.table_name,
            "column_name": self.column_name or "",
            "check_type": self.check_type,
            "observed_value": str(self.observed_value),
            "expected_value": str(self.expected_value),
            "status": self.status,
            "severity": self.severity,
            "details": self.details,
            "executed_at": self.executed_at,
        }


# -------------------------------------------------------------------------
# Pure Python Validation Evaluators (Unit-Testable without Spark)
# -------------------------------------------------------------------------
def evaluate_null_percentage(
    null_count: int,
    total_count: int,
    max_allowed_pct: float,
    severity: str = "CRITICAL"
) -> Tuple[str, float, str]:
    """Evaluates whether null percentage is within tolerance."""
    if total_count == 0:
        return "PASS", 0.0, "Total count is zero; check skipped."

    null_pct = round((null_count / total_count) * 100, 2)
    if null_pct <= max_allowed_pct:
        return "PASS", null_pct, f"Null percentage {null_pct}% <= allowed {max_allowed_pct}%."
    else:
        status = "FAIL" if severity == "CRITICAL" else "WARN"
        return status, null_pct, f"Null percentage {null_pct}% exceeds allowed {max_allowed_pct}%."


def evaluate_uniqueness(
    total_count: int,
    distinct_count: int
) -> Tuple[str, int, str]:
    """Evaluates whether a business key contains duplicates."""
    duplicate_count = total_count - distinct_count
    if duplicate_count == 0:
        return "PASS", 0, "All keys are unique (0 duplicates)."
    else:
        return "FAIL", duplicate_count, f"Found {duplicate_count} duplicate keys."


def evaluate_numeric_range(
    observed_min: float,
    observed_max: float,
    allowed_min: float,
    allowed_max: float
) -> Tuple[str, str]:
    """Evaluates whether values fall within realistic domain boundaries."""
    if observed_min < allowed_min or observed_max > allowed_max:
        return "FAIL", f"Observed range [{observed_min}, {observed_max}] outside permitted [{allowed_min}, {allowed_max}]."
    return "PASS", f"Observed range [{observed_min}, {observed_max}] within permitted [{allowed_min}, {allowed_max}]."


def evaluate_categorical_domain(
    observed_values: Set[str],
    allowed_values: Set[str]
) -> Tuple[str, Set[str], str]:
    """Evaluates whether all observed categories exist in the allowed domain."""
    invalid_values = observed_values - allowed_values
    if not invalid_values:
        return "PASS", set(), "All observed categories are valid."
    return "FAIL", invalid_values, f"Encountered unexpected values: {sorted(list(invalid_values))}."


def evaluate_row_count_reconciliation(
    source_count: int,
    target_count: int,
    max_variance_pct: float = 0.0
) -> Tuple[str, float, str]:
    """Evaluates row count parity between ingestion phases."""
    if source_count == 0:
        return "WARN", 0.0, "Source count is 0."

    variance = abs(source_count - target_count)
    variance_pct = round((variance / source_count) * 100, 2)
    if variance_pct <= max_variance_pct:
        return "PASS", variance_pct, f"Reconciliation verified. Variance: {variance_pct}% (Diff={variance})."
    return "FAIL", variance_pct, f"Reconciliation mismatch! Variance: {variance_pct}% exceeds {max_variance_pct}%."


def evaluate_default_rate_sanity(
    observed_default_rate_pct: float,
    min_pct: float = 1.0,
    max_pct: float = 60.0
) -> Tuple[str, str]:
    """Sanity checks portfolio/cohort default rate against banking benchmarks."""
    if min_pct <= observed_default_rate_pct <= max_pct:
        return "PASS", f"Default rate {observed_default_rate_pct}% is within realistic risk bounds [{min_pct}%, {max_pct}%]."
    return "FAIL", f"Default rate {observed_default_rate_pct}% is unrealistic (expected between {min_pct}% and {max_pct}%)."


# -------------------------------------------------------------------------
# PySpark Data Quality Runner
# -------------------------------------------------------------------------
class DataQualityEngine:
    """Executes Medallion data quality checks on PySpark DataFrames."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.results: List[DataQualityResult] = []

    def check_bronze_layer(self, raw_source_count: int, bronze_df: DataFrame) -> List[DataQualityResult]:
        """Runs checks on Bronze raw ingestion table."""
        logger.info("Executing Bronze Layer Data Quality checks...")
        table_name = "bronze_loans"
        bronze_count = bronze_df.count()

        # 1. Source to Bronze row count reconciliation
        status, var_pct, details = evaluate_row_count_reconciliation(raw_source_count, bronze_count)
        self.results.append(DataQualityResult(
            check_name="bronze_source_reconciliation",
            table_name=table_name,
            column_name=None,
            check_type="RECONCILIATION",
            observed_value=str(bronze_count),
            expected_value=str(raw_source_count),
            status=status,
            severity="CRITICAL",
            details=details,
        ))

        # 2. Corrupt record percentage check
        corrupt_count = bronze_df.filter(F.col("_corrupt_record").isNotNull()).count()
        status, corrupt_pct, details = evaluate_null_percentage(
            corrupt_count, bronze_count, max_allowed_pct=1.0, severity="WARNING"
        )
        self.results.append(DataQualityResult(
            check_name="bronze_corrupt_records_threshold",
            table_name=table_name,
            column_name="_corrupt_record",
            check_type="MALFORMED_RECORDS",
            observed_value=f"{corrupt_count} ({corrupt_pct}%)",
            expected_value="<= 1.0%",
            status=status,
            severity="WARNING",
            details=f"Identified {corrupt_count} corrupt CSV lines.",
        ))

        return self.results

    def check_silver_layer(
        self,
        bronze_count: int,
        silver_accepted: DataFrame,
        silver_quarantine: DataFrame
    ) -> List[DataQualityResult]:
        """Runs checks on Silver Cleaned and Quarantine tables."""
        logger.info("Executing Silver Layer Data Quality checks...")
        accepted_count = silver_accepted.count()
        quarantine_count = silver_quarantine.count()
        table_name = "silver_loans"

        # 1. Total conservation check: Accepted + Quarantine == Bronze
        total_accounted = accepted_count + quarantine_count
        recon_status = "PASS" if total_accounted == bronze_count else "FAIL"
        self.results.append(DataQualityResult(
            check_name="silver_record_conservation_reconciliation",
            table_name=table_name,
            column_name=None,
            check_type="RECONCILIATION",
            observed_value=f"Accepted({accepted_count}) + Quarantine({quarantine_count}) = {total_accounted}",
            expected_value=f"Bronze Count = {bronze_count}",
            status=recon_status,
            severity="CRITICAL",
            details=f"Record conservation between Bronze and Silver layers (Difference={bronze_count - total_accounted}).",
        ))

        # 2. Primary key uniqueness on silver_loans
        distinct_ids = silver_accepted.select("loan_id").distinct().count()
        status, dups, details = evaluate_uniqueness(accepted_count, distinct_ids)
        self.results.append(DataQualityResult(
            check_name="silver_loan_id_uniqueness",
            table_name=table_name,
            column_name="loan_id",
            check_type="UNIQUENESS",
            observed_value=f"Distinct={distinct_ids}, Total={accepted_count}",
            expected_value="100% Unique (0 duplicates)",
            status=status,
            severity="CRITICAL",
            details=details,
        ))

        # 3. Critical column null checks
        critical_cols = ["loan_id", "loan_amount", "annual_income", "interest_rate_pct", "loan_grade", "loan_status"]
        for col in critical_cols:
            null_count = silver_accepted.filter(F.col(col).isNull()).count()
            status, pct, details = evaluate_null_percentage(null_count, accepted_count, max_allowed_pct=0.0, severity="CRITICAL")
            self.results.append(DataQualityResult(
                check_name=f"silver_null_check_{col}",
                table_name=table_name,
                column_name=col,
                check_type="NULL_CHECK",
                observed_value=f"{null_count} nulls ({pct}%)",
                expected_value="0 nulls (0.0%)",
                status=status,
                severity="CRITICAL",
                details=details,
            ))

        # 4. Numeric range check for loan_amount
        min_amt, max_amt = silver_accepted.agg(F.min("loan_amount"), F.max("loan_amount")).collect()[0]
        status, details = evaluate_numeric_range(
            float(min_amt or 0), float(max_amt or 0),
            self.config.business_rules.min_loan_amount,
            self.config.business_rules.max_loan_amount
        )
        self.results.append(DataQualityResult(
            check_name="silver_range_check_loan_amount",
            table_name=table_name,
            column_name="loan_amount",
            check_type="RANGE_CHECK",
            observed_value=f"[{min_amt}, {max_amt}]",
            expected_value=f"[{self.config.business_rules.min_loan_amount}, {self.config.business_rules.max_loan_amount}]",
            status=status,
            severity="CRITICAL",
            details=details,
        ))

        # 5. Loan Grade domain integrity check
        observed_grades = {row["loan_grade"] for row in silver_accepted.select("loan_grade").distinct().collect()}
        status, invalid, details = evaluate_categorical_domain(
            observed_grades, set(self.config.business_rules.valid_grades)
        )
        self.results.append(DataQualityResult(
            check_name="silver_domain_check_loan_grade",
            table_name=table_name,
            column_name="loan_grade",
            check_type="DOMAIN_CHECK",
            observed_value=str(sorted(list(observed_grades))),
            expected_value=str(sorted(self.config.business_rules.valid_grades)),
            status=status,
            severity="CRITICAL",
            details=details,
        ))

        return self.results

    def check_gold_layer(
        self,
        gold_grade_df: DataFrame,
        gold_portfolio_df: DataFrame
    ) -> List[DataQualityResult]:
        """Runs checks on Gold Analytical Marts."""
        logger.info("Executing Gold Layer Data Quality checks...")
        table_name = "gold_analytics"

        # 1. Default rate sanity check across portfolio
        portfolio_metrics = gold_portfolio_df.collect()[0]
        overall_default_rate = float(portfolio_metrics["overall_default_rate_pct"])
        status, details = evaluate_default_rate_sanity(
            overall_default_rate,
            self.config.data_quality.min_expected_default_rate_pct,
            self.config.data_quality.max_expected_default_rate_pct
        )
        self.results.append(DataQualityResult(
            check_name="gold_portfolio_default_rate_sanity",
            table_name=table_name,
            column_name="overall_default_rate_pct",
            check_type="SANITY_CHECK",
            observed_value=f"{overall_default_rate}%",
            expected_value=f"[{self.config.data_quality.min_expected_default_rate_pct}%, {self.config.data_quality.max_expected_default_rate_pct}%]",
            status=status,
            severity="WARNING",
            details=details,
        ))

        # 2. Exposure consistency check between Gold Grade Mart and Portfolio Summary
        sum_grade_exposure = gold_grade_df.agg(F.round(F.sum("total_loan_exposure"), 2)).collect()[0][0]
        portfolio_exposure = float(portfolio_metrics["total_exposure_amount"])
        exposure_diff = round(abs(float(sum_grade_exposure) - portfolio_exposure), 2)
        status = "PASS" if exposure_diff <= 0.05 else "FAIL"
        self.results.append(DataQualityResult(
            check_name="gold_exposure_cross_table_consistency",
            table_name=table_name,
            column_name="total_loan_exposure",
            check_type="CONSISTENCY_CHECK",
            observed_value=f"GradeSum={sum_grade_exposure}, Portfolio={portfolio_exposure} (Diff={exposure_diff})",
            expected_value="Diff <= $0.05",
            status=status,
            severity="CRITICAL",
            details="Verified total portfolio exposure matches aggregate sum of grade loan exposures.",
        ))

        return self.results

    def get_results_dataframe(self) -> DataFrame:
        """Converts collected results into a PySpark DataFrame."""
        rows = [r.to_dict() for r in self.results]
        return self.spark.createDataFrame(rows, schema=DATA_QUALITY_RESULT_SCHEMA)

    def save_results_to_delta(self, target: str, write_mode: str = "append") -> None:
        """Saves Data Quality report rows to Delta Lake."""
        df = self.get_results_dataframe()
        logger.info(f"Writing {df.count()} Data Quality check results to: {target}")
        if "/" in target or "\\" in target:
            df.write.format("delta").mode(write_mode).save(target)
        else:
            df.write.format("delta").mode(write_mode).saveAsTable(target)

    def has_critical_failures(self) -> bool:
        """Returns True if any check with CRITICAL severity failed."""
        return any(r.status == "FAIL" and r.severity == "CRITICAL" for r in self.results)
