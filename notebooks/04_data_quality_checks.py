# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04: Data Quality & Audit Framework
# MAGIC ### Credit Risk Analytics Pipeline — Azure Databricks & Delta Lake
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Execute rigorous data quality verification across Bronze, Silver, and Gold layers.
# MAGIC - Perform row-count reconciliation, primary key uniqueness tests, and schema integrity validation.
# MAGIC - Evaluate numerical range limits and categorical domain constraints.
# MAGIC - Verify mathematical exposure consistency across Gold analytical marts.
# MAGIC - Persist a comprehensive audit scorecard into Delta Lake.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Load Delta Layers
bronze_df = spark.read.format("delta").load("data/delta/bronze/loans")
silver_accepted = spark.read.format("delta").load("data/delta/silver/loans")
silver_quarantine = spark.read.format("delta").load("data/delta/silver/loans_quarantine")
gold_grade = spark.read.format("delta").load("data/delta/gold/default_rate_by_grade")
gold_portfolio = spark.read.format("delta").load("data/delta/gold/loan_portfolio_summary")

dq_results = []

def record_check(name, table, col, check_type, observed, expected, status, severity, details):
    dq_results.append({
        "check_name": name,
        "table_name": table,
        "column_name": col or "",
        "check_type": check_type,
        "observed_value": str(observed),
        "expected_value": str(expected),
        "status": status,
        "severity": severity,
        "details": details,
        "executed_at": datetime.utcnow()
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Bronze & Silver Reconciliation Checks

# COMMAND ----------

bronze_count = bronze_df.count()
accepted_count = silver_accepted.count()
quarantine_count = silver_quarantine.count()
total_silver = accepted_count + quarantine_count

# Check 1: Record Conservation
recon_status = "PASS" if total_silver == bronze_count else "FAIL"
record_check(
    name="silver_record_conservation_reconciliation",
    table="silver_loans",
    col=None,
    check_type="RECONCILIATION",
    observed=f"Accepted({accepted_count}) + Quarantine({quarantine_count}) = {total_silver}",
    expected=f"Bronze Count = {bronze_count}",
    status=recon_status,
    severity="CRITICAL",
    details=f"Reconciled record conservation across Silver tables. Variance = {bronze_count - total_silver}."
)

# Check 2: Primary Key Uniqueness on Silver Accepted
distinct_ids = silver_accepted.select("loan_id").distinct().count()
dups = accepted_count - distinct_ids
uniq_status = "PASS" if dups == 0 else "FAIL"
record_check(
    name="silver_loan_id_uniqueness",
    table="silver_loans",
    col="loan_id",
    check_type="UNIQUENESS",
    observed=f"Distinct={distinct_ids}, Total={accepted_count}",
    expected="0 duplicates",
    status=uniq_status,
    severity="CRITICAL",
    details=f"Primary key deduplication validation (Duplicates={dups})."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Critical Field Null Checks

# COMMAND ----------

critical_fields = ["loan_id", "loan_amount", "annual_income", "interest_rate_pct", "loan_grade", "loan_status"]

for col in critical_fields:
    null_count = silver_accepted.filter(F.col(col).isNull()).count()
    null_pct = round((null_count / accepted_count) * 100, 2)
    col_status = "PASS" if null_count == 0 else "FAIL"
    record_check(
        name=f"silver_null_check_{col}",
        table="silver_loans",
        col=col,
        check_type="NULL_CHECK",
        observed=f"{null_count} ({null_pct}%)",
        expected="0 (0.0%)",
        status=col_status,
        severity="CRITICAL",
        details=f"Verified critical underwriting attribute {col} contains zero nulls."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Business Range and Domain Checks

# COMMAND ----------

# Loan amount range check
min_loan, max_loan = silver_accepted.agg(F.min("loan_amount"), F.max("loan_amount")).collect()[0]
range_status = "PASS" if (min_loan >= 500 and max_loan <= 100000) else "FAIL"
record_check(
    name="silver_range_check_loan_amount",
    table="silver_loans",
    col="loan_amount",
    check_type="RANGE_CHECK",
    observed=f"[{min_loan}, {max_loan}]",
    expected="[$500, $100,000]",
    status=range_status,
    severity="CRITICAL",
    details="Verified loan amount complies with portfolio underwriting policy limits."
)

# Loan grade domain check
observed_grades = {r["loan_grade"] for r in silver_accepted.select("loan_grade").distinct().collect()}
allowed_grades = {"A", "B", "C", "D", "E", "F", "G"}
domain_status = "PASS" if observed_grades.issubset(allowed_grades) else "FAIL"
record_check(
    name="silver_domain_check_loan_grade",
    table="silver_loans",
    col="loan_grade",
    check_type="DOMAIN_CHECK",
    observed=str(sorted(list(observed_grades))),
    expected=str(sorted(list(allowed_grades))),
    status=domain_status,
    severity="CRITICAL",
    details="Verified loan grade matches valid credit rating agencies categories."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Gold Mart Consistency Checks

# COMMAND ----------

portfolio_row = gold_portfolio.collect()[0]
portfolio_exposure = float(portfolio_row["total_exposure_amount"])
portfolio_default_rate = float(portfolio_row["overall_default_rate_pct"])
grade_sum_exposure = float(gold_grade.agg(F.round(F.sum("total_loan_exposure"), 2)).collect()[0][0])
exposure_diff = round(abs(grade_sum_exposure - portfolio_exposure), 2)

consistency_status = "PASS" if exposure_diff <= 0.05 else "FAIL"
record_check(
    name="gold_exposure_cross_table_consistency",
    table="gold_analytics",
    col="total_loan_exposure",
    check_type="CONSISTENCY_CHECK",
    observed=f"GradeMart={grade_sum_exposure}, Portfolio={portfolio_exposure} (Diff={exposure_diff})",
    expected="Diff <= $0.05",
    status=consistency_status,
    severity="CRITICAL",
    details="Verified mathematical reconciliation between sum of grade exposure and portfolio total."
)

sanity_status = "PASS" if (1.0 <= portfolio_default_rate <= 60.0) else "FAIL"
record_check(
    name="gold_portfolio_default_rate_sanity",
    table="gold_analytics",
    col="overall_default_rate_pct",
    check_type="SANITY_CHECK",
    observed=f"{portfolio_default_rate}%",
    expected="[1.0%, 60.0%]",
    status=sanity_status,
    severity="WARNING",
    details="Sanity check of portfolio default rate against commercial lending benchmarks."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Build Scorecard DataFrame & Persist to Delta Lake

# COMMAND ----------

dq_schema = StructType([
    StructField("check_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("check_type", StringType(), False),
    StructField("observed_value", StringType(), True),
    StructField("expected_value", StringType(), True),
    StructField("status", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("details", StringType(), True),
    StructField("executed_at", TimestampType(), False),
])

dq_df = spark.createDataFrame(dq_results, schema=dq_schema)
dq_df.write.format("delta").mode("append").save("data/delta/audit/dq_validation_results")

print("=" * 80)
print(f"DATA QUALITY SCORECARD ({len(dq_results)} checks executed)")
print("=" * 80)
display(dq_df)
