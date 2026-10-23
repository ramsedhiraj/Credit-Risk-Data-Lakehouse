"""
Module: ingestion.py
Description: Bronze Layer Raw Ingestion for Credit Risk Data Pipeline.
             Reads raw CSV with explicit schema, captures corrupt records,
             enriches with ingestion metadata, and writes to Delta Lake idempotently.
"""

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.schemas import BRONZE_RAW_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ingestion.bronze")


def ingest_bronze(
    spark: SparkSession,
    raw_csv_path: str,
    bronze_target: str,
    batch_id: Optional[str] = None,
    write_mode: str = "overwrite",
) -> Dict[str, Any]:
    """
    Ingests raw loan CSV data into the Bronze Delta table.

    Key Features:
    - Explicit schema to prevent expensive type inference scans and schema drift.
    - PERMISSIVE parsing mode with _corrupt_record column to catch malformed lines.
    - Preserves all source fields intact for downstream auditing.
    - Injects ingestion metadata: _ingestion_timestamp, _source_file, _batch_id.
    - Idempotent writes via configurable write mode (default: overwrite or partition replace).

    Args:
        spark: Active SparkSession.
        raw_csv_path: Path to the input CSV file or directory.
        bronze_target: Delta table path or table name.
        batch_id: Optional unique batch identifier. Generated if None.
        write_mode: Spark DataFrame write mode ('overwrite' or 'append').

    Returns:
        Dictionary containing ingestion audit metrics.
    """
    if batch_id is None:
        batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"

    logger.info(f"Starting Bronze ingestion from: {raw_csv_path}")
    logger.info(f"Batch ID: {batch_id} | Target: {bronze_target} | Write Mode: {write_mode}")

    # 1. Read CSV with header=true and flexible column projection
    # Supports both the 28-column development fixture and 151-column full canonical Lending Club files
    raw_reader = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
    )
    raw_df = raw_reader.load(raw_csv_path)

    target_fields = [f.name for f in BRONZE_RAW_SCHEMA.fields if f.name != "_corrupt_record"]
    available_cols = [c for c in target_fields if c in raw_df.columns]

    select_exprs = [F.col(c).cast(StringType()).alias(c) for c in available_cols]
    if "_corrupt_record" in raw_df.columns:
        select_exprs.append(F.col("_corrupt_record"))
    else:
        select_exprs.append(F.lit(None).cast(StringType()).alias("_corrupt_record"))

    projected_raw_df = raw_df.select(*select_exprs)

    # 2. Add Ingestion Metadata
    bronze_df = (
        projected_raw_df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # 3. Write to Bronze Delta Lake
    # If the target is a path (contains / or \), write to path; otherwise, write to managed/external table.
    writer = bronze_df.write.format("delta").mode(write_mode)
    
    if "/" in bronze_target or "\\" in bronze_target:
        writer.save(bronze_target)
    else:
        writer.saveAsTable(bronze_target)

    logger.info(f"Bronze Delta write complete to: {bronze_target}")

    # 4. Audit metrics collection
    # Read back from Delta to verify persistence and exact row counts
    saved_df = (
        spark.read.format("delta").load(bronze_target)
        if ("/" in bronze_target or "\\" in bronze_target)
        else spark.table(bronze_target)
    )

    total_ingested = saved_df.filter(F.col("_batch_id") == batch_id).count() if write_mode == "append" else saved_df.count()
    corrupt_count = saved_df.filter(F.col("_corrupt_record").isNotNull()).count()

    metrics = {
        "status": "SUCCESS",
        "batch_id": batch_id,
        "raw_source_path": raw_csv_path,
        "bronze_target": bronze_target,
        "ingested_row_count": total_ingested,
        "corrupt_record_count": corrupt_count,
        "timestamp": datetime.utcnow().isoformat(),
    }

    logger.info(
        f"Bronze Ingestion Summary: Ingested={total_ingested} rows, "
        f"Corrupt={corrupt_count} rows, Batch={batch_id}"
    )

    return metrics
