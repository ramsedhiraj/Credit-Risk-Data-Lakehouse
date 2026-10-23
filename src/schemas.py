"""
Module: schemas.py
Description: Explicit PySpark StructType schemas for Bronze raw ingestion,
             Silver cleaned and quarantined data, and Data Quality audit logs.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# -------------------------------------------------------------------------
# Bronze Layer: Explicit Raw Ingestion Schema
# Uses StringType for source fields to allow resilient ingestion of raw text
# without silent truncation or crashing, capturing malformed rows in _corrupt_record.
# -------------------------------------------------------------------------
BRONZE_RAW_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("loan_amnt", StringType(), True),
    StructField("funded_amnt", StringType(), True),
    StructField("term", StringType(), True),
    StructField("int_rate", StringType(), True),
    StructField("installment", StringType(), True),
    StructField("grade", StringType(), True),
    StructField("sub_grade", StringType(), True),
    StructField("emp_title", StringType(), True),
    StructField("emp_length", StringType(), True),
    StructField("home_ownership", StringType(), True),
    StructField("annual_inc", StringType(), True),
    StructField("verification_status", StringType(), True),
    StructField("issue_d", StringType(), True),
    StructField("loan_status", StringType(), True),
    StructField("purpose", StringType(), True),
    StructField("addr_state", StringType(), True),
    StructField("dti", StringType(), True),
    StructField("delinq_2yrs", StringType(), True),
    StructField("earliest_cr_line", StringType(), True),
    StructField("open_acc", StringType(), True),
    StructField("pub_rec", StringType(), True),
    StructField("revol_bal", StringType(), True),
    StructField("revol_util", StringType(), True),
    StructField("total_acc", StringType(), True),
    StructField("total_pymnt", StringType(), True),
    StructField("recoveries", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# -------------------------------------------------------------------------
# Silver Layer: Cleaned and Standardized Schema
# Standardized naming (snake_case), typed fields, parsed metrics, and risk flags.
# -------------------------------------------------------------------------
SILVER_ACCEPTED_SCHEMA = StructType([
    StructField("loan_id", StringType(), False),
    StructField("borrower_id", StringType(), True),
    StructField("loan_amount", DoubleType(), False),
    StructField("funded_amount", DoubleType(), True),
    StructField("term_months", IntegerType(), True),
    StructField("interest_rate_pct", DoubleType(), False),
    StructField("installment_amount", DoubleType(), True),
    StructField("loan_grade", StringType(), False),
    StructField("loan_subgrade", StringType(), True),
    StructField("employment_title", StringType(), True),
    StructField("employment_length_years", IntegerType(), True),
    StructField("home_ownership_status", StringType(), True),
    StructField("annual_income", DoubleType(), False),
    StructField("income_verification_status", StringType(), True),
    StructField("issue_date", DateType(), True),
    StructField("loan_status", StringType(), False),
    StructField("loan_purpose", StringType(), True),
    StructField("borrower_state", StringType(), True),
    StructField("debt_to_income_ratio", DoubleType(), True),
    StructField("delinquency_2yrs_count", IntegerType(), True),
    StructField("earliest_credit_line_date", StringType(), True),
    StructField("open_accounts_count", IntegerType(), True),
    StructField("derogatory_records_count", IntegerType(), True),
    StructField("revolving_balance", DoubleType(), True),
    StructField("revolving_utilization_pct", DoubleType(), True),
    StructField("total_accounts_count", IntegerType(), True),
    StructField("total_payment_received", DoubleType(), True),
    StructField("gross_recoveries", DoubleType(), True),
    # Business-derived credit risk columns
    StructField("is_default", IntegerType(), False),
    StructField("is_resolved", IntegerType(), False),
    StructField("income_band", StringType(), False),
    # Ingestion & Audit lineage metadata
    StructField("_ingestion_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), True),
    StructField("_batch_id", StringType(), True),
    StructField("_silver_processed_at", TimestampType(), False),
])

# -------------------------------------------------------------------------
# Silver Layer: Quarantine Schema
# Captures malformed records with explicit failure reason and audit metadata.
# -------------------------------------------------------------------------
SILVER_QUARANTINE_SCHEMA = StructType([
    StructField("raw_id", StringType(), True),
    StructField("raw_loan_amnt", StringType(), True),
    StructField("raw_annual_inc", StringType(), True),
    StructField("raw_int_rate", StringType(), True),
    StructField("raw_grade", StringType(), True),
    StructField("raw_loan_status", StringType(), True),
    StructField("raw_addr_state", StringType(), True),
    StructField("rejection_reasons", StringType(), False),
    StructField("_batch_id", StringType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_rejected_at", TimestampType(), False),
])

# -------------------------------------------------------------------------
# Data Quality Audit Result Schema
# -------------------------------------------------------------------------
DATA_QUALITY_RESULT_SCHEMA = StructType([
    StructField("check_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("check_type", StringType(), False),
    StructField("observed_value", StringType(), True),
    StructField("expected_value", StringType(), True),
    StructField("status", StringType(), False),     # PASS | WARN | FAIL
    StructField("severity", StringType(), False),   # CRITICAL | WARNING
    StructField("details", StringType(), True),
    StructField("executed_at", TimestampType(), False),
])
