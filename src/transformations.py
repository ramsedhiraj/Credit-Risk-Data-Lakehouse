"""
Module: transformations.py
Description: Silver Layer Cleaning, Standardization, Deduplication, and Quarantine Logic.
             Transforms Bronze raw data into high-integrity Silver Delta tables while
             routing invalid and duplicate records into a structured quarantine table.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from src.config import PipelineConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("transformations.silver")


def clean_numeric_string(col_name: str) -> F.Column:
    """Removes percentage signs, currency symbols, and whitespace, then casts to Double."""
    cleaned = F.regexp_replace(F.trim(F.col(col_name)), r"[%\$,\s]", "")
    return F.when(cleaned != "", cleaned.cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType()))


def parse_term_months(col_name: str) -> F.Column:
    """Extracts integer months from strings such as ' 36 months' or '60'."""
    extracted = F.regexp_extract(F.col(col_name), r"(\d+)", 1)
    return F.when(extracted != "", extracted.cast(IntegerType())).otherwise(F.lit(None).cast(IntegerType()))


def parse_employment_length(col_name: str) -> F.Column:
    """Standardizes emp_length strings into integer years (<1 -> 0, 10+ -> 10, n/a -> null)."""
    val = F.trim(F.col(col_name))
    return (
        F.when(val.contains("10+"), F.lit(10))
        .when(val.contains("< 1"), F.lit(0))
        .when(val.isin("n/a", "NA", ""), F.lit(None).cast(IntegerType()))
        .otherwise(F.regexp_extract(val, r"(\d+)", 1).cast(IntegerType()))
    )


def parse_issue_date(col_name: str) -> F.Column:
    """
    Parses flexible date formats (e.g. 'Jan-2016', 'Jan-16', '2016-01-01')
    into standardized first-of-month DateType (yyyy-MM-01).
    """
    cleaned = F.trim(F.col(col_name))
    return (
        F.when(cleaned.rlike(r"^[A-Za-z]{3}-\d{4}$"), F.to_date(F.concat(F.lit("01-"), cleaned), "dd-MMM-yyyy"))
        .when(cleaned.rlike(r"^[A-Za-z]{3}-\d{2}$"), F.to_date(F.concat(F.lit("01-"), cleaned), "dd-MMM-yy"))
        .when(cleaned.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(cleaned, "yyyy-MM-dd"))
        .otherwise(F.lit(None).cast(DateType()))
    )


def derive_income_band(income_col: str) -> F.Column:
    """Bins annual income into documented credit underwriting risk tiers."""
    return (
        F.when(F.col(income_col) < 30000, F.lit("1. Low (<$30k)"))
        .when(F.col(income_col) < 60000, F.lit("2. Lower-Middle ($30k-$60k)"))
        .when(F.col(income_col) < 100000, F.lit("3. Upper-Middle ($60k-$100k)"))
        .when(F.col(income_col) < 150000, F.lit("4. High ($100k-$150k)"))
        .otherwise(F.lit("5. Very High (>$150k)"))
    )


def transform_silver(
    spark: SparkSession,
    bronze_df: DataFrame,
    config: PipelineConfig,
) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
    """
    Transforms Bronze raw loan DataFrame into Silver Accepted and Silver Quarantine DataFrames.

    Processing Steps:
    1. Parse and cast raw string fields to standardized types.
    2. Standardize column names to lower_snake_case.
    3. Profile and flag business rule violations (invalid amounts, income, grades, etc.).
    4. Deduplicate on business primary key (loan_id) using window function ranking.
    5. Route valid records to Silver Accepted and invalid/duplicate records to Quarantine.
    6. Derive credit risk flags: is_default, is_resolved, income_band.

    Returns:
        Tuple of (silver_accepted_df, silver_quarantine_df, metrics_dict)
    """
    total_bronze_count = bronze_df.count()
    logger.info(f"Starting Silver transformation on {total_bronze_count} Bronze rows...")

    rules = config.business_rules

    # Step 1: Base casting and column standardization
    parsed_df = (
        bronze_df
        .withColumn("loan_id", F.trim(F.col("id")))
        .withColumn("borrower_id", F.trim(F.col("member_id")))
        .withColumn("loan_amount", clean_numeric_string("loan_amnt"))
        .withColumn("funded_amount", clean_numeric_string("funded_amnt"))
        .withColumn("term_months", parse_term_months("term"))
        .withColumn("interest_rate_pct", clean_numeric_string("int_rate"))
        .withColumn("installment_amount", clean_numeric_string("installment"))
        .withColumn("loan_grade", F.upper(F.trim(F.col("grade"))))
        .withColumn("loan_subgrade", F.upper(F.trim(F.col("sub_grade"))))
        .withColumn(
            "employment_title",
            F.when(F.trim(F.col("emp_title")) != "", F.trim(F.col("emp_title"))).otherwise(F.lit(None).cast(StringType()))
        )
        .withColumn("employment_length_years", parse_employment_length("emp_length"))
        .withColumn("home_ownership_status", F.upper(F.trim(F.col("home_ownership"))))
        .withColumn("annual_income", clean_numeric_string("annual_inc"))
        .withColumn("income_verification_status", F.trim(F.col("verification_status")))
        .withColumn("issue_date", parse_issue_date("issue_d"))
        .withColumn("loan_status", F.trim(F.col("loan_status")))
        .withColumn("loan_purpose", F.lower(F.trim(F.col("purpose"))))
        .withColumn("borrower_state", F.upper(F.trim(F.col("addr_state"))))
        .withColumn("debt_to_income_ratio", clean_numeric_string("dti"))
        .withColumn("delinquency_2yrs_count", clean_numeric_string("delinq_2yrs").cast(IntegerType()))
        .withColumn("earliest_credit_line_date", F.trim(F.col("earliest_cr_line")))
        .withColumn("open_accounts_count", clean_numeric_string("open_acc").cast(IntegerType()))
        .withColumn("derogatory_records_count", clean_numeric_string("pub_rec").cast(IntegerType()))
        .withColumn("revolving_balance", clean_numeric_string("revol_bal"))
        .withColumn("revolving_utilization_pct", clean_numeric_string("revol_util"))
        .withColumn("total_accounts_count", clean_numeric_string("total_acc").cast(IntegerType()))
        .withColumn("total_payment_received", clean_numeric_string("total_pymnt"))
        .withColumn("gross_recoveries", clean_numeric_string("recoveries"))
    )

    # Step 2: Deduplication using Window ranking
    # Retain the most recently ingested record if duplicate loan_ids exist
    window_spec = Window.partitionBy("loan_id").orderBy(F.col("_ingestion_timestamp").desc())
    ranked_df = parsed_df.withColumn("_row_num", F.row_number().over(window_spec))

    # Step 3: Construct field-specific validation failure conditions
    # Build list of rejection flags
    rejection_conds = [
        F.when(F.col("loan_id").isNull() | (F.col("loan_id") == ""), F.lit("NULL_OR_EMPTY_LOAN_ID")),
        F.when(
            F.col("loan_amount").isNull()
            | (F.col("loan_amount") < rules.min_loan_amount)
            | (F.col("loan_amount") > rules.max_loan_amount),
            F.lit("INVALID_LOAN_AMOUNT")
        ),
        F.when(
            F.col("annual_income").isNull()
            | (F.col("annual_income") < rules.min_annual_income)
            | (F.col("annual_income") > rules.max_annual_income),
            F.lit("INVALID_ANNUAL_INCOME")
        ),
        F.when(
            F.col("interest_rate_pct").isNull()
            | (F.col("interest_rate_pct") < rules.min_interest_rate_pct)
            | (F.col("interest_rate_pct") > rules.max_interest_rate_pct),
            F.lit("INVALID_INTEREST_RATE")
        ),
        F.when(~F.col("loan_grade").isin(rules.valid_grades), F.lit("INVALID_LOAN_GRADE")),
        F.when(F.col("loan_status").isNull() | (F.col("loan_status") == ""), F.lit("NULL_LOAN_STATUS")),
        F.when(F.col("_row_num") > 1, F.lit("DUPLICATE_LOAN_ID")),
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("CORRUPT_RAW_RECORD")),
    ]

    # Combine rejection reasons into an array, removing nulls
    validated_df = ranked_df.withColumn(
        "rejection_reasons_arr",
        F.array_compact(F.array(*rejection_conds))
    ).withColumn(
        "is_quarantined",
        F.size(F.col("rejection_reasons_arr")) > 0
    )

    # Step 4: Split into Accepted and Quarantine
    # 4a. Silver Accepted
    silver_accepted = (
        validated_df
        .filter(~F.col("is_quarantined"))
        # Add Credit Risk derived fields
        .withColumn(
            "is_default",
            F.when(F.col("loan_status").isin(rules.default_statuses), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "is_resolved",
            F.when(F.col("loan_status").isin(rules.resolved_statuses), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("income_band", derive_income_band("annual_income"))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .select(
            "loan_id", "borrower_id", "loan_amount", "funded_amount", "term_months",
            "interest_rate_pct", "installment_amount", "loan_grade", "loan_subgrade",
            "employment_title", "employment_length_years", "home_ownership_status",
            "annual_income", "income_verification_status", "issue_date", "loan_status",
            "loan_purpose", "borrower_state", "debt_to_income_ratio", "delinquency_2yrs_count",
            "earliest_credit_line_date", "open_accounts_count", "derogatory_records_count",
            "revolving_balance", "revolving_utilization_pct", "total_accounts_count",
            "total_payment_received", "gross_recoveries",
            "is_default", "is_resolved", "income_band",
            "_ingestion_timestamp", "_source_file", "_batch_id", "_silver_processed_at"
        )
    )

    # 4b. Silver Quarantine
    silver_quarantine = (
        validated_df
        .filter(F.col("is_quarantined"))
        .withColumn("rejection_reasons", F.concat_ws("; ", F.col("rejection_reasons_arr")))
        .withColumn("_rejected_at", F.current_timestamp())
        .select(
            F.col("id").alias("raw_id"),
            F.col("loan_amnt").alias("raw_loan_amnt"),
            F.col("annual_inc").alias("raw_annual_inc"),
            F.col("int_rate").alias("raw_int_rate"),
            F.col("grade").alias("raw_grade"),
            F.col("loan_status").alias("raw_loan_status"),
            F.col("addr_state").alias("raw_addr_state"),
            "rejection_reasons",
            "_batch_id",
            "_source_file",
            "_rejected_at"
        )
    )

    accepted_count = silver_accepted.count()
    quarantine_count = silver_quarantine.count()
    duplicate_count = validated_df.filter(F.col("_row_num") > 1).count()

    metrics = {
        "status": "SUCCESS",
        "total_bronze_records": total_bronze_count,
        "silver_accepted_records": accepted_count,
        "silver_quarantined_records": quarantine_count,
        "duplicate_records_detected": duplicate_count,
        "reconciliation_check": (accepted_count + quarantine_count == total_bronze_count),
        "processed_at": datetime.utcnow().isoformat(),
    }

    logger.info(
        f"Silver Transformation Summary: Bronze={total_bronze_count} -> "
        f"Accepted={accepted_count}, Quarantined={quarantine_count} "
        f"(Duplicates={duplicate_count})"
    )

    return silver_accepted, silver_quarantine, metrics


def save_silver_tables(
    spark: SparkSession,
    silver_accepted: DataFrame,
    silver_quarantine: DataFrame,
    config: PipelineConfig,
    write_mode: str = "overwrite",
) -> None:
    """Persists Accepted and Quarantine DataFrames to Delta Lake."""
    accepted_target = config.get_table_target("silver_loans")
    quarantine_target = config.get_table_target("silver_quarantine")

    logger.info(f"Saving Silver Accepted to: {accepted_target}")
    if "/" in accepted_target or "\\" in accepted_target:
        silver_accepted.write.format("delta").mode(write_mode).save(accepted_target)
    else:
        silver_accepted.write.format("delta").mode(write_mode).saveAsTable(accepted_target)

    logger.info(f"Saving Silver Quarantine to: {quarantine_target}")
    if "/" in quarantine_target or "\\" in quarantine_target:
        silver_quarantine.write.format("delta").mode(write_mode).save(quarantine_target)
    else:
        silver_quarantine.write.format("delta").mode(write_mode).saveAsTable(quarantine_target)

    logger.info("Silver Delta tables successfully persisted.")
