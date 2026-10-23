# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01: Bronze Layer Ingestion
# MAGIC ### Credit Risk Analytics Pipeline — Azure Databricks & Delta Lake
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Ingest raw Lending Club loan CSV records into the Bronze Delta table.
# MAGIC - Enforce an explicit schema to prevent silent data type coercion and schema drift.
# MAGIC - Capture malformed or corrupt rows using `_corrupt_record` (PERMISSIVE mode).
# MAGIC - Enrich records with audit metadata (`_ingestion_timestamp`, `_source_file`, `_batch_id`).
# MAGIC - Support idempotent reruns via configurable write modes.

# COMMAND ----------

# Imports & Helper initialization
import uuid
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Try to fetch Databricks widget parameters; fallback to project defaults
try:
    dbutils.widgets.text("raw_data_path", "data/raw/loan_data_sample.csv")
    dbutils.widgets.text("bronze_target", "data/delta/bronze/loans")
    dbutils.widgets.text("write_mode", "overwrite")
    raw_data_path = dbutils.widgets.get("raw_data_path")
    bronze_target = dbutils.widgets.get("bronze_target")
    write_mode = dbutils.widgets.get("write_mode")
except NameError:
    raw_data_path = "data/raw/loan_data_sample.csv"
    bronze_target = "data/delta/bronze/loans"
    write_mode = "overwrite"

batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
print(f"Executing Bronze Ingestion:")
print(f"  Raw Source:   {raw_data_path}")
print(f"  Target Delta: {bronze_target}")
print(f"  Batch ID:     {batch_id}")
print(f"  Write Mode:   {write_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define Explicit Raw Schema
# MAGIC Explicit schemas prevent expensive distributed full-file schema inference scans
# MAGIC and ensure that raw text records are faithfully ingested for auditing.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Raw CSV with Corrupt Record Tracking

# COMMAND ----------

raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(BRONZE_RAW_SCHEMA)
    .load(raw_data_path)
)

bronze_df = (
    raw_df
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_batch_id", F.lit(batch_id))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Persist to Bronze Delta Lake Table

# COMMAND ----------

writer = bronze_df.write.format("delta").mode(write_mode)
if "/" in bronze_target or "\\" in bronze_target:
    writer.save(bronze_target)
else:
    writer.saveAsTable(bronze_target)

print(f"Bronze Delta Lake write completed to {bronze_target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Ingestion Reconciliation & Verification Query

# COMMAND ----------

bronze_delta = (
    spark.read.format("delta").load(bronze_target)
    if ("/" in bronze_target or "\\" in bronze_target)
    else spark.table(bronze_target)
)

total_count = bronze_delta.count()
corrupt_count = bronze_delta.filter(F.col("_corrupt_record").isNotNull()).count()

print(f"Bronze Layer Reconciliation:")
print(f"  Total Ingested Rows: {total_count}")
print(f"  Corrupt Rows:        {corrupt_count}")

display(bronze_delta.limit(5))
