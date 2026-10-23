# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02: Silver Layer Cleaning, Deduplication & Quarantine
# MAGIC ### Credit Risk Analytics Pipeline — Azure Databricks & Delta Lake
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Standardize column names to lower_snake_case.
# MAGIC - Cast string attributes to typed numeric, date, and categorical representations.
# MAGIC - Eliminate duplicate loan records using business key `loan_id` via window ranking.
# MAGIC - Enforce strict business underwriting validations (loan amounts, income, grades, interest rates).
# MAGIC - Route invalid records to `silver_loans_quarantine` with descriptive rejection reasons.
# MAGIC - Derive credit risk flags: `is_default`, `is_resolved`, and `income_band`.

# COMMAND ----------

# Imports & Helper initialization
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, IntegerType, DateType, StringType

try:
    dbutils.widgets.text("bronze_path", "data/delta/bronze/loans")
    dbutils.widgets.text("silver_accepted_path", "data/delta/silver/loans")
    dbutils.widgets.text("silver_quarantine_path", "data/delta/silver/loans_quarantine")
    bronze_path = dbutils.widgets.get("bronze_path")
    silver_accepted_path = dbutils.widgets.get("silver_accepted_path")
    silver_quarantine_path = dbutils.widgets.get("silver_quarantine_path")
except NameError:
    bronze_path = "data/delta/bronze/loans"
    silver_accepted_path = "data/delta/silver/loans"
    silver_quarantine_path = "data/delta/silver/loans_quarantine"

# Load Bronze Delta Table
bronze_df = (
    spark.read.format("delta").load(bronze_path)
    if ("/" in bronze_path or "\\" in bronze_path)
    else spark.table(bronze_path)
)
print(f"Loaded {bronze_df.count()} rows from Bronze: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Clean Numeric Strings & Parse Date Fields

# COMMAND ----------

def clean_num(col_name):
    cleaned = F.regexp_replace(F.trim(F.col(col_name)), r"[%\$,\s]", "")
    return F.when(cleaned != "", cleaned.cast(DoubleType())).otherwise(F.lit(None).cast(DoubleType()))

def parse_term(col_name):
    return F.regexp_extract(F.col(col_name), r"(\d+)", 1).cast(IntegerType())

def parse_emp_len(col_name):
    val = F.trim(F.col(col_name))
    return (
        F.when(val.contains("10+"), F.lit(10))
        .when(val.contains("< 1"), F.lit(0))
        .when(val.isin("n/a", "NA", ""), F.lit(None).cast(IntegerType()))
        .otherwise(F.regexp_extract(val, r"(\d+)", 1).cast(IntegerType()))
    )

def parse_issue_dt(col_name):
    cleaned = F.trim(F.col(col_name))
    return (
        F.when(cleaned.rlike(r"^[A-Za-z]{3}-\d{4}$"), F.to_date(F.concat(F.lit("01-"), cleaned), "dd-MMM-yyyy"))
        .when(cleaned.rlike(r"^[A-Za-z]{3}-\d{2}$"), F.to_date(F.concat(F.lit("01-"), cleaned), "dd-MMM-yy"))
        .when(cleaned.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(cleaned, "yyyy-MM-dd"))
        .otherwise(F.lit(None).cast(DateType()))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Execute Cleaning and Deduplication

# COMMAND ----------

parsed_df = (
    bronze_df
    .withColumn("loan_id", F.trim(F.col("id")))
    .withColumn("borrower_id", F.trim(F.col("member_id")))
    .withColumn("loan_amount", clean_num("loan_amnt"))
    .withColumn("funded_amount", clean_num("funded_amnt"))
    .withColumn("term_months", parse_term("term"))
    .withColumn("interest_rate_pct", clean_num("int_rate"))
    .withColumn("installment_amount", clean_num("installment"))
    .withColumn("loan_grade", F.upper(F.trim(F.col("grade"))))
    .withColumn("loan_subgrade", F.upper(F.trim(F.col("sub_grade"))))
    .withColumn("employment_title", F.when(F.trim(F.col("emp_title")) != "", F.trim(F.col("emp_title"))).otherwise(F.lit(None).cast(StringType())))
    .withColumn("employment_length_years", parse_emp_len("emp_length"))
    .withColumn("home_ownership_status", F.upper(F.trim(F.col("home_ownership"))))
    .withColumn("annual_income", clean_num("annual_inc"))
    .withColumn("income_verification_status", F.trim(F.col("verification_status")))
    .withColumn("issue_date", parse_issue_dt("issue_d"))
    .withColumn("loan_status", F.trim(F.col("loan_status")))
    .withColumn("loan_purpose", F.lower(F.trim(F.col("purpose"))))
    .withColumn("borrower_state", F.upper(F.trim(F.col("addr_state"))))
    .withColumn("debt_to_income_ratio", clean_num("dti"))
    .withColumn("delinquency_2yrs_count", clean_num("delinq_2yrs").cast(IntegerType()))
    .withColumn("earliest_credit_line_date", F.trim(F.col("earliest_cr_line")))
    .withColumn("open_accounts_count", clean_num("open_acc").cast(IntegerType()))
    .withColumn("derogatory_records_count", clean_num("pub_rec").cast(IntegerType()))
    .withColumn("revolving_balance", clean_num("revol_bal"))
    .withColumn("revolving_utilization_pct", clean_num("revol_util"))
    .withColumn("total_accounts_count", clean_num("total_acc").cast(IntegerType()))
    .withColumn("total_payment_received", clean_num("total_pymnt"))
    .withColumn("gross_recoveries", clean_num("recoveries"))
)

# Deduplication Window: rank by ingestion timestamp descending for each loan_id
window_spec = Window.partitionBy("loan_id").orderBy(F.col("_ingestion_timestamp").desc())
ranked_df = parsed_df.withColumn("_row_num", F.row_number().over(window_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Business Rule Validation and Quarantine Routing

# COMMAND ----------

valid_grades = ["A", "B", "C", "D", "E", "F", "G"]

rejection_conds = [
    F.when(F.col("loan_id").isNull() | (F.col("loan_id") == ""), F.lit("NULL_OR_EMPTY_LOAN_ID")),
    F.when(F.col("loan_amount").isNull() | (F.col("loan_amount") < 500) | (F.col("loan_amount") > 100000), F.lit("INVALID_LOAN_AMOUNT")),
    F.when(F.col("annual_income").isNull() | (F.col("annual_income") < 1000) | (F.col("annual_income") > 10000000), F.lit("INVALID_ANNUAL_INCOME")),
    F.when(F.col("interest_rate_pct").isNull() | (F.col("interest_rate_pct") < 0) | (F.col("interest_rate_pct") > 100), F.lit("INVALID_INTEREST_RATE")),
    F.when(~F.col("loan_grade").isin(valid_grades), F.lit("INVALID_LOAN_GRADE")),
    F.when(F.col("loan_status").isNull() | (F.col("loan_status") == ""), F.lit("NULL_LOAN_STATUS")),
    F.when(F.col("_row_num") > 1, F.lit("DUPLICATE_LOAN_ID")),
    F.when(F.col("_corrupt_record").isNotNull(), F.lit("CORRUPT_RAW_RECORD")),
]

validated_df = ranked_df.withColumn(
    "rejection_reasons_arr", F.array_compact(F.array(*rejection_conds))
).withColumn(
    "is_quarantined", F.size(F.col("rejection_reasons_arr")) > 0
)

# Derive Credit Risk Categorizations
silver_accepted = (
    validated_df.filter(~F.col("is_quarantined"))
    .withColumn("is_default", F.when(F.col("loan_status").isin("Charged Off", "Default"), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("is_resolved", F.when(F.col("loan_status").isin("Fully Paid", "Charged Off", "Default"), F.lit(1)).otherwise(F.lit(0)))
    .withColumn(
        "income_band",
        F.when(F.col("annual_income") < 30000, F.lit("1. Low (<$30k)"))
        .when(F.col("annual_income") < 60000, F.lit("2. Lower-Middle ($30k-$60k)"))
        .when(F.col("annual_income") < 100000, F.lit("3. Upper-Middle ($60k-$100k)"))
        .when(F.col("annual_income") < 150000, F.lit("4. High ($100k-$150k)"))
        .otherwise(F.lit("5. Very High (>$150k)"))
    )
    .withColumn("_silver_processed_at", F.current_timestamp())
    .drop("_row_num", "_corrupt_record", "rejection_reasons_arr", "is_quarantined")
)

silver_quarantine = (
    validated_df.filter(F.col("is_quarantined"))
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Persist to Silver Delta Tables

# COMMAND ----------

# Save Accepted
accepted_writer = silver_accepted.write.format("delta").mode("overwrite")
if "/" in silver_accepted_path or "\\" in silver_accepted_path:
    accepted_writer.save(silver_accepted_path)
else:
    accepted_writer.saveAsTable(silver_accepted_path)

# Save Quarantine
quarantine_writer = silver_quarantine.write.format("delta").mode("overwrite")
if "/" in silver_quarantine_path or "\\" in silver_quarantine_path:
    quarantine_writer.save(silver_quarantine_path)
else:
    quarantine_writer.saveAsTable(silver_quarantine_path)

print(f"Silver Accepted written to: {silver_accepted_path} ({silver_accepted.count()} rows)")
print(f"Silver Quarantine written to: {silver_quarantine_path} ({silver_quarantine.count()} rows)")
display(silver_accepted.limit(5))
