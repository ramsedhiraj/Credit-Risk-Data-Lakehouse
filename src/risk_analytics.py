"""
Module: risk_analytics.py
Description: Gold Layer Credit Risk Analytical Marts for Credit Risk Pipeline.
             Implements default rate by grade, state, and income band,
             overall portfolio summary, and advanced window function analytics.
"""

import logging
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.config import PipelineConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("analytics.gold")


def compute_default_rate_by_grade(silver_df: DataFrame) -> DataFrame:
    """
    Gold Mart 1: Default Rate and Exposure by Loan Grade.
    Distinguishes between cohort resolved default rate and total active default rate.
    """
    logger.info("Computing Gold Mart: Default Rate by Loan Grade...")
    return (
        silver_df
        .groupBy("loan_grade")
        .agg(
            F.count("*").alias("total_loans"),
            F.sum("is_default").alias("defaulted_loans"),
            F.sum(F.when(F.col("loan_status") == "Fully Paid", 1).otherwise(0)).alias("fully_paid_loans"),
            F.sum(F.when(F.col("is_resolved") == 0, 1).otherwise(0)).alias("active_loans"),
            F.round(
                F.when(F.sum("is_resolved") > 0, F.sum("is_default") / F.sum("is_resolved") * 100).otherwise(0.0), 2
            ).alias("cohort_default_rate_pct"),
            F.round(F.sum("is_default") / F.count("*") * 100, 2).alias("total_default_rate_pct"),
            F.round(F.sum("loan_amount"), 2).alias("total_loan_exposure"),
            F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
            F.round(F.avg("interest_rate_pct"), 2).alias("avg_interest_rate_pct"),
            F.round(F.sum("gross_recoveries"), 2).alias("total_gross_recoveries"),
        )
        .orderBy("loan_grade")
    )


def compute_default_rate_by_state(silver_df: DataFrame, min_sample_size: int = 10) -> DataFrame:
    """
    Gold Mart 2: Default Rate by State with Sample Size Safeguard.
    Applies dense_rank() over qualified states to prevent small sample distortion.
    """
    logger.info(f"Computing Gold Mart: Default Rate by State (Min Sample={min_sample_size})...")
    state_agg = (
        silver_df
        .groupBy("borrower_state")
        .agg(
            F.count("*").alias("total_loans"),
            F.sum("is_default").alias("defaulted_loans"),
            F.round(F.sum("is_default") / F.count("*") * 100, 2).alias("default_rate_pct"),
            F.round(F.sum("loan_amount"), 2).alias("total_loan_exposure"),
            F.round(F.avg("annual_income"), 2).alias("avg_annual_income"),
        )
        .withColumn(
            "has_sufficient_sample_size",
            F.when(F.col("total_loans") >= min_sample_size, F.lit(True)).otherwise(F.lit(False))
        )
    )

    # Window function: rank qualified states by default rate descending
    qualified_window = Window.partitionBy("has_sufficient_sample_size").orderBy(F.desc("default_rate_pct"), F.desc("total_loans"))

    return (
        state_agg
        .withColumn(
            "state_risk_rank",
            F.when(F.col("has_sufficient_sample_size"), F.dense_rank().over(qualified_window)).otherwise(F.lit(None))
        )
        .orderBy(F.desc("has_sufficient_sample_size"), F.col("state_risk_rank").asc_nulls_last())
    )


def compute_default_rate_by_income_band(silver_df: DataFrame) -> DataFrame:
    """
    Gold Mart 3: Default Rate by Income Band.
    Analyzes credit risk distribution across borrower earning brackets.
    """
    logger.info("Computing Gold Mart: Default Rate by Income Band...")
    return (
        silver_df
        .groupBy("income_band")
        .agg(
            F.count("*").alias("total_loans"),
            F.sum("is_default").alias("defaulted_loans"),
            F.round(F.sum("is_default") / F.count("*") * 100, 2).alias("default_rate_pct"),
            F.round(F.sum("loan_amount"), 2).alias("total_loan_exposure"),
            F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
            F.round(F.avg("debt_to_income_ratio"), 2).alias("avg_debt_to_income_ratio"),
        )
        .withColumn(
            "sort_order",
            F.when(F.col("income_band").contains("1. Low"), 1)
            .when(F.col("income_band").contains("2. Lower-Middle"), 2)
            .when(F.col("income_band").contains("3. Upper-Middle"), 3)
            .when(F.col("income_band").contains("4. High"), 4)
            .otherwise(5)
        )
        .orderBy("sort_order")
        .drop("sort_order")
    )


def compute_portfolio_summary(silver_df: DataFrame) -> DataFrame:
    """
    Gold Mart 4: High-Level Portfolio Executive Summary.
    Single-row KPI summary of volume, exposure, default rates, and recoveries.
    """
    logger.info("Computing Gold Mart: Loan Portfolio Summary...")
    return silver_df.agg(
        F.count("*").alias("total_portfolio_loans"),
        F.round(F.sum("loan_amount"), 2).alias("total_exposure_amount"),
        F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
        F.round(F.avg("interest_rate_pct"), 2).alias("avg_interest_rate_pct"),
        F.round(F.sum("is_default") / F.count("*") * 100, 2).alias("overall_default_rate_pct"),
        F.round(
            F.when(F.sum("is_resolved") > 0, F.sum("is_default") / F.sum("is_resolved") * 100).otherwise(0.0), 2
        ).alias("resolved_cohort_default_rate_pct"),
        F.sum("is_default").alias("total_defaulted_loans"),
        F.sum(F.when(F.col("loan_status") == "Fully Paid", 1).otherwise(0)).alias("total_fully_paid_loans"),
        F.sum(F.when(F.col("is_resolved") == 0, 1).otherwise(0)).alias("total_active_loans"),
        F.round(F.sum("total_payment_received"), 2).alias("total_payments_received"),
        F.round(F.sum("gross_recoveries"), 2).alias("total_gross_recoveries"),
    )


def compute_window_analytics(silver_df: DataFrame) -> DataFrame:
    """
    Gold Mart 5: Advanced Window Function Risk Analytics.
    Demonstrates:
    - Cumulative running totals of loan volume and exposure partitioned by loan grade over issue date.
    - Partitioned ranking of loan volume within each grade and issue cohort.
    - Percent contribution of each cohort to total grade exposure.
    """
    logger.info("Computing Gold Mart: Advanced Window Function Analytics...")
    
    # Pre-aggregate by loan grade and issue month
    monthly_grade_df = (
        silver_df
        .filter(F.col("issue_date").isNotNull())
        .withColumn("issue_month", F.date_trunc("month", F.col("issue_date")))
        .groupBy("loan_grade", "issue_month")
        .agg(
            F.count("*").alias("monthly_loans_issued"),
            F.round(F.sum("loan_amount"), 2).alias("monthly_exposure_issued"),
            F.sum("is_default").alias("monthly_defaults"),
        )
    )

    # Window 1: Running total over time partitioned by loan_grade
    running_window = (
        Window
        .partitionBy("loan_grade")
        .orderBy("issue_month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Window 2: Partitioned by loan_grade across all time to calculate total grade exposure
    grade_total_window = Window.partitionBy("loan_grade")

    # Window 3: Dense rank by monthly loan volume within each grade
    rank_window = Window.partitionBy("loan_grade").orderBy(F.desc("monthly_loans_issued"))

    return (
        monthly_grade_df
        .withColumn("cumulative_loans_issued", F.sum("monthly_loans_issued").over(running_window))
        .withColumn("cumulative_exposure_issued", F.round(F.sum("monthly_exposure_issued").over(running_window), 2))
        .withColumn(
            "grade_lifetime_exposure",
            F.round(F.sum("monthly_exposure_issued").over(grade_total_window), 2)
        )
        .withColumn(
            "monthly_exposure_pct_of_grade",
            F.round(
                F.col("monthly_exposure_issued") / F.col("grade_lifetime_exposure") * 100, 2
            )
        )
        .withColumn("volume_rank_within_grade", F.dense_rank().over(rank_window))
        .orderBy("loan_grade", "issue_month")
    )


def save_gold_tables(
    spark: SparkSession,
    grade_df: DataFrame,
    state_df: DataFrame,
    income_band_df: DataFrame,
    portfolio_df: DataFrame,
    window_df: DataFrame,
    config: PipelineConfig,
    write_mode: str = "overwrite",
) -> None:
    """Persists all Gold Delta Lake tables."""
    tables = [
        ("gold_default_by_grade", grade_df),
        ("gold_default_by_state", state_df),
        ("gold_default_by_income_band", income_band_df),
        ("gold_portfolio_summary", portfolio_df),
        ("gold_window_analytics", window_df),
    ]

    for alias, df in tables:
        target = config.get_table_target(alias)
        logger.info(f"Persisting Gold Delta table [{alias}] to: {target}")
        if "/" in target or "\\" in target:
            df.write.format("delta").mode(write_mode).save(target)
        else:
            df.write.format("delta").mode(write_mode).saveAsTable(target)

    logger.info("All Gold Delta tables successfully persisted.")
