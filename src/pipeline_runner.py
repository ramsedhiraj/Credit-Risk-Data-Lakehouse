"""
Module: pipeline_runner.py
Description: End-to-end Orchestration Runner for the Credit Risk Data Pipeline.
             Executes Bronze Ingestion -> Silver Transformation -> Gold Analytics
             and validates every layer using the Data Quality Framework.
"""

import argparse
import os
import sys
import time
from datetime import datetime

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.config import PipelineConfig
from src.data_quality import DataQualityEngine
from src.ingestion import ingest_bronze
from src.risk_analytics import (
    compute_default_rate_by_grade,
    compute_default_rate_by_income_band,
    compute_default_rate_by_state,
    compute_portfolio_summary,
    compute_window_analytics,
    save_gold_tables,
)
from src.transformations import save_silver_tables, transform_silver


def create_spark_session(app_name: str = "CreditRiskDataPipeline") -> SparkSession:
    """Builds a local SparkSession configured for Delta Lake."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_pipeline(config_path: str = "configs/project_config.example.json") -> bool:
    start_time = time.time()
    print("=" * 80)
    print("CREDIT RISK ANALYTICS PIPELINE - AZURE DATABRICKS & DELTA LAKE")
    print("=" * 80)

    # 1. Load Configuration
    config = PipelineConfig.from_json(config_path)
    print(f"Loaded Configuration from: {config_path}")
    print(f"Environment: {config.environment}")
    print(f"Raw Input Source: {config.storage.raw_data_path}")

    # 2. Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    dq_engine = DataQualityEngine(spark, config)

    try:
        # Determine raw row count from CSV (excluding header)
        with open(config.storage.raw_data_path, "r", encoding="utf-8") as f:
            raw_source_count = sum(1 for _ in f) - 1

        print(f"\n[PHASE 1] Bronze Ingestion...")
        bronze_metrics = ingest_bronze(
            spark=spark,
            raw_csv_path=config.storage.raw_data_path,
            bronze_target=config.get_table_target("bronze_loans"),
            write_mode="overwrite",
        )
        print(f"  -> Ingested {bronze_metrics['ingested_row_count']} rows into Bronze Delta table.")

        # Read back Bronze DataFrame for downstream processing
        bronze_df = spark.read.format("delta").load(config.get_table_target("bronze_loans"))
        dq_engine.check_bronze_layer(raw_source_count, bronze_df)

        print(f"\n[PHASE 2] Silver Cleaning, Deduplication & Quarantine...")
        silver_accepted, silver_quarantine, silver_metrics = transform_silver(
            spark=spark,
            bronze_df=bronze_df,
            config=config,
        )

        save_silver_tables(
            spark=spark,
            silver_accepted=silver_accepted,
            silver_quarantine=silver_quarantine,
            config=config,
            write_mode="overwrite",
        )
        print(f"  -> Accepted Rows: {silver_metrics['silver_accepted_records']}")
        print(f"  -> Quarantined Rows: {silver_metrics['silver_quarantined_records']}")
        print(f"  -> Duplicates Removed: {silver_metrics['duplicate_records_detected']}")

        dq_engine.check_silver_layer(bronze_df.count(), silver_accepted, silver_quarantine)

        print(f"\n[PHASE 3] Gold Risk Analytics Marts...")
        grade_mart = compute_default_rate_by_grade(silver_accepted)
        state_mart = compute_default_rate_by_state(silver_accepted, min_sample_size=config.business_rules.min_state_sample_size)
        income_band_mart = compute_default_rate_by_income_band(silver_accepted)
        portfolio_mart = compute_portfolio_summary(silver_accepted)
        window_mart = compute_window_analytics(silver_accepted)

        save_gold_tables(
            spark=spark,
            grade_df=grade_mart,
            state_df=state_mart,
            income_band_df=income_band_mart,
            portfolio_df=portfolio_mart,
            window_df=window_mart,
            config=config,
            write_mode="overwrite",
        )

        dq_engine.check_gold_layer(grade_mart, portfolio_mart)

        # 3. Print Key Results Tables
        print("\n" + "=" * 80)
        print("EXECUTIVE PORTFOLIO SUMMARY (Gold Mart)")
        print("=" * 80)
        portfolio_mart.show(truncate=False)

        print("\nDEFAULT RATE BY LOAN GRADE (Gold Mart)")
        grade_mart.select(
            "loan_grade", "total_loans", "defaulted_loans", "cohort_default_rate_pct",
            "total_default_rate_pct", "total_loan_exposure", "avg_interest_rate_pct"
        ).show(truncate=False)

        print("\nDEFAULT RATE BY INCOME BAND (Gold Mart)")
        income_band_mart.show(truncate=False)

        print("\nDEFAULT RATE BY TOP 5 STATES (Gold Mart - Min Sample >= 10)")
        state_mart.filter("has_sufficient_sample_size = true").limit(5).show(truncate=False)

        # 4. Save and Show Data Quality Scorecard
        print("\n" + "=" * 80)
        print("DATA QUALITY SCORECARD")
        print("=" * 80)
        dq_results_df = dq_engine.get_results_dataframe()
        dq_results_df.select("check_name", "check_type", "status", "severity", "observed_value", "expected_value").show(30, truncate=False)

        dq_engine.save_results_to_delta(config.get_table_target("dq_validation_results"), write_mode="append")
        print(f"Data Quality results logged to: {config.get_table_target('dq_validation_results')}")

        has_critical = dq_engine.has_critical_failures()
        duration = round(time.time() - start_time, 2)
        print(f"\nPipeline finished in {duration} seconds.")

        if has_critical:
            print("ALERT: One or more CRITICAL Data Quality checks failed!")
            return False
        else:
            print("SUCCESS: All pipeline phases and Data Quality checks completed successfully.")
            return True

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Credit Risk Data Pipeline")
    parser.add_argument("--config", default="configs/project_config.example.json", help="Path to config file")
    args = parser.parse_args()

    success = run_pipeline(args.config)
    sys.exit(0 if success else 1)
