# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03: Gold Layer Credit Risk Analytics & Marts
# MAGIC ### Credit Risk Analytics Pipeline — Azure Databricks & Delta Lake
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Generate business-ready Delta tables for credit risk reporting and dashboards.
# MAGIC - Compute default rates by loan grade, borrower state, and income brackets.
# MAGIC - Provide an executive portfolio summary with Basel-compliant cohort default metrics.
# MAGIC - Demonstrate advanced Spark SQL window functions (running totals, cohort ranks, rolling averages).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

try:
    dbutils.widgets.text("silver_path", "data/delta/silver/loans")
    dbutils.widgets.text("gold_root", "data/delta/gold")
    silver_path = dbutils.widgets.get("silver_path")
    gold_root = dbutils.widgets.get("gold_root")
except NameError:
    silver_path = "data/delta/silver/loans"
    gold_root = "data/delta/gold"

# Load Silver Cleaned Delta Table
silver_df = (
    spark.read.format("delta").load(silver_path)
    if ("/" in silver_path or "\\" in silver_path)
    else spark.table(silver_path)
)
print(f"Loaded {silver_df.count()} clean records from Silver: {silver_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Default Rate by Loan Grade
# MAGIC - Distinguishes between resolved cohort default rate and total active portfolio default rate.

# COMMAND ----------

gold_grade = (
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
display(gold_grade)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Default Rate by State with Minimum Sample-Size Safeguard
# MAGIC - Flags states with fewer than 10 loans to prevent skewed default percentages.
# MAGIC - Applies `dense_rank()` across statistically valid states.

# COMMAND ----------

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
        F.when(F.col("total_loans") >= 10, F.lit(True)).otherwise(F.lit(False))
    )
)

qualified_window = Window.partitionBy("has_sufficient_sample_size").orderBy(F.desc("default_rate_pct"), F.desc("total_loans"))

gold_state = (
    state_agg
    .withColumn(
        "state_risk_rank",
        F.when(F.col("has_sufficient_sample_size"), F.dense_rank().over(qualified_window)).otherwise(F.lit(None))
    )
    .orderBy(F.desc("has_sufficient_sample_size"), F.col("state_risk_rank").asc_nulls_last())
)
display(gold_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Default Rate by Income Band

# COMMAND ----------

gold_income_band = (
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
    .orderBy("income_band")
)
display(gold_income_band)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Executive Portfolio Summary

# COMMAND ----------

gold_portfolio_summary = silver_df.agg(
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
display(gold_portfolio_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Window Function Risk Analytics (Running Totals, Cohort Ranks)

# COMMAND ----------

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

running_window = Window.partitionBy("loan_grade").orderBy("issue_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
grade_total_window = Window.partitionBy("loan_grade")
rank_window = Window.partitionBy("loan_grade").orderBy(F.desc("monthly_loans_issued"))

gold_window_analytics = (
    monthly_grade_df
    .withColumn("cumulative_loans_issued", F.sum("monthly_loans_issued").over(running_window))
    .withColumn("cumulative_exposure_issued", F.round(F.sum("monthly_exposure_issued").over(running_window), 2))
    .withColumn("grade_lifetime_exposure", F.round(F.sum("monthly_exposure_issued").over(grade_total_window), 2))
    .withColumn("monthly_exposure_pct_of_grade", F.round(F.col("monthly_exposure_issued") / F.col("grade_lifetime_exposure") * 100, 2))
    .withColumn("volume_rank_within_grade", F.dense_rank().over(rank_window))
    .orderBy("loan_grade", "issue_month")
)
display(gold_window_analytics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Persist Gold Tables to Delta Lake

# COMMAND ----------

gold_tables = [
    ("default_rate_by_grade", gold_grade),
    ("default_rate_by_state", gold_state),
    ("default_rate_by_income_band", gold_income_band),
    ("loan_portfolio_summary", gold_portfolio_summary),
    ("loan_window_analytics", gold_window_analytics),
]

for name, df in gold_tables:
    target_path = f"{gold_root}/{name}"
    df.write.format("delta").mode("overwrite").save(target_path)
    print(f"Persisted Gold Mart: {target_path}")
