# Credit Risk Analytics Pipeline — Architecture & Technical Design

## 1. Executive Summary & Business Context

In consumer credit lending and banking, evaluating portfolio risk, probability of default (PD), and exposure at default (EAD) requires high-integrity data pipelines that comply with regulatory standards (e.g., Basel II/III, IFRS 9, CECL). 

This project implements an end-to-end, production-grade **Credit Risk Data Lakehouse** on **Azure Databricks, PySpark, Spark SQL, and Delta Lake** leveraging the **Medallion Architecture**. The pipeline ingests loan origination and performance data, rigorously cleans and standardizes attributes, quarantines non-compliant records, executes credit risk aggregations, and applies window functions for cohort risk analysis.

---

## 2. Medallion Lakehouse Architecture

The pipeline follows the Lakehouse Medallion pattern across three Delta Lake tiers:

```mermaid
flowchart TD
    subgraph Ingestion ["Ingestion Tier"]
        CSV["Raw Loan CSV Source<br/>(Lending Club / ADLS Gen2)"]
    end

    subgraph BronzeLayer ["Bronze Layer (Raw Ingestion)"]
        Bronze["Delta Table: bronze_loans<br/>• Explicit StructType Schema<br/>• Ingestion Metadata (_batch_id, _timestamp)<br/>• PERMISSIVE parse (_corrupt_record)"]
    end

    subgraph SilverLayer ["Silver Layer (Cleaning & Governance)"]
        Clean["Data Standardization Engine<br/>• Type Casting & Symbol Stripping<br/>• Window Deduplication on loan_id<br/>• Underwriting Policy Validations"]
        SilverAcc["Delta Table: silver_loans<br/>• Validated & Typed Records<br/>• Derived Flags (is_default, is_resolved)<br/>• Income Risk Tiers"]
        SilverQuar["Delta Table: silver_loans_quarantine<br/>• Rejected / Corrupt Records<br/>• Rejection Reason Audit Trails"]
    end

    subgraph GoldLayer ["Gold Layer (Credit Risk Marts)"]
        Mart1["gold_default_rate_by_grade<br/>(Cohort & Total Default Rates, Exposure)"]
        Mart2["gold_default_rate_by_state<br/>(Dense Rank, Sample-Size Safeguard)"]
        Mart3["gold_default_rate_by_income_band<br/>(Earning Brackets vs DTI & Risk)"]
        Mart4["gold_loan_portfolio_summary<br/>(Executive KPI Scorecard)"]
        Mart5["gold_loan_window_analytics<br/>(Running Totals, Cohort Exposure %)"]
    end

    subgraph AuditLayer ["Audit & Governance"]
        DQEngine["Data Quality Engine<br/>• Row Count Reconciliation<br/>• Key Uniqueness<br/>• Numeric Ranges & Domains"]
        DQTable["Delta Table: dq_validation_results<br/>(Audit Scorecard & Compliance Log)"]
    end

    CSV -->|Explicit Schema Read| Bronze
    Bronze -->|Clean & Deduplicate| Clean
    Clean -->|Pass Validation| SilverAcc
    Clean -->|Fail Validation| SilverQuar
    SilverAcc -->|Aggregate| Mart1
    SilverAcc -->|Aggregate| Mart2
    SilverAcc -->|Aggregate| Mart3
    SilverAcc -->|Aggregate| Mart4
    SilverAcc -->|Window Functions| Mart5
    Bronze -.-> DQEngine
    SilverAcc -.-> DQEngine
    SilverQuar -.-> DQEngine
    GoldLayer -.-> DQEngine
    DQEngine --> DQTable
```

---

## 3. Layer Specifications

### 3.1. Bronze Layer (Raw Ingestion)
- **Table Name:** `bronze_loans`
- **Format:** Delta Lake
- **Storage Target:** `data/delta/bronze/loans` (Local) / `abfss://lakehouse@<account>.dfs.core.windows.net/bronze/loans` (Azure)
- **Ingestion Strategy:**
  - Enforces an explicit `StructType` schema (`BRONZE_RAW_SCHEMA`) to eliminate costly schema inference scans and protect against silent type coercion.
  - Configures `mode: PERMISSIVE` with `columnNameOfCorruptRecord: _corrupt_record` to capture malformed CSV rows without aborting the pipeline.
  - Appends system audit metadata:
    - `_ingestion_timestamp`: UTC timestamp of execution.
    - `_source_file`: Fully-qualified URI of the source raw file (`input_file_name()`).
    - `_batch_id`: Unique ingestion execution identifier (`batch_YYYYMMDDHHMMSS_<uuid>`).
- **Idempotency:** Reruns with identical batch configurations use partition replacement or deterministic table overwrites to prevent duplicate record accumulation.

### 3.2. Silver Layer (Cleaning, Deduplication & Quarantine)
- **Table Names:** `silver_loans` (Accepted), `silver_loans_quarantine` (Rejected)
- **Format:** Delta Lake
- **Storage Target:** `data/delta/silver/loans` and `data/delta/silver/loans_quarantine`
- **Transformation Logic:**
  1. **Column Standardization:** Converts attributes to snake_case (`id` $\rightarrow$ `loan_id`, `loan_amnt` $\rightarrow$ `loan_amount`, `addr_state` $\rightarrow$ `borrower_state`).
  2. **Type Casting & Text Parsing:**
     - Regex cleaning strips `%`, `$`, commas, and whitespace from interest rates, balances, and loan amounts.
     - `term` (e.g. `' 36 months'`) is extracted to integer `term_months` (36, 60).
     - `emp_length` (e.g. `'10+ years'`, `'< 1 year'`) is converted to integer `employment_length_years` (10, 0, null).
     - Flexible date parsing handles both `'MMM-yyyy'` (e.g. `'Jan-2016'`) and ISO `'yyyy-MM-dd'` into `DateType` (`yyyy-MM-01`).
  3. **Window Deduplication:**
     - Primary business key: `loan_id`.
     - Window specification: `ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY _ingestion_timestamp DESC)`.
     - Ensures the most recently ingested record is retained; duplicates (`row_num > 1`) are flagged and routed to quarantine.
  4. **Underwriting Validation Rules:**
     - `loan_id IS NOT NULL AND loan_id != ''`
     - `loan_amount BETWEEN 500.0 AND 100000.0`
     - `annual_income BETWEEN 1000.0 AND 10000000.0`
     - `interest_rate_pct BETWEEN 0.0 AND 100.0`
     - `loan_grade IN ('A', 'B', 'C', 'D', 'E', 'F', 'G')`
     - `loan_status IS NOT NULL AND loan_status != ''`
     - `_corrupt_record IS NULL`
  5. **Quarantine Routing:**
     - Invalid records are separated into `silver_loans_quarantine` with semi-colon delimited `rejection_reasons` (e.g., `INVALID_LOAN_AMOUNT; DUPLICATE_LOAN_ID`) and `_rejected_at` timestamp.
  6. **Credit Risk Enrichment:**
     - `is_default`: Binary indicator (`1` if `loan_status IN ('Charged Off', 'Default')`, else `0`).
     - `is_resolved`: Binary indicator (`1` if `loan_status IN ('Fully Paid', 'Charged Off', 'Default')`, else `0`).
     - `income_band`: Binned into five risk tiers (`< $30k`, `$30k-$60k`, `$60k-$100k`, `$100k-$150k`, `> $150k`).

### 3.3. Gold Layer (Credit Risk Marts)
- **Table Names:**
  - `gold_default_rate_by_grade`
  - `gold_default_rate_by_state`
  - `gold_default_rate_by_income_band`
  - `gold_loan_portfolio_summary`
  - `gold_loan_window_analytics`
- **Format:** Delta Lake
- **Key Business Metrics:**
  - **Cohort Default Rate vs Total Default Rate:**
    $$\text{Cohort Default Rate (\%)} = \frac{\sum \text{is\_default}}{\sum \text{is\_resolved}} \times 100$$
    $$\text{Total Default Rate (\%)} = \frac{\sum \text{is\_default}}{\text{Total Loans}} \times 100$$
    *Banking Rationale:* Active loans (`Current`, `Late 31-120 days`, `In Grace Period`) have not reached maturity. Cohort default rate measures actual loss rate among concluded loans, preventing artificial dilution.
  - **Statistical Sample-Size Safeguards:**
    States with fewer than 10 loans are flagged (`has_sufficient_sample_size = false`), ensuring small sample noise does not distort geographic risk rankings.
  - **Advanced Window Analytics:**
    - Running cumulative loans and exposure: `SUM(monthly_loans) OVER (PARTITION BY loan_grade ORDER BY issue_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
    - Cohort percentage of lifetime grade exposure: `monthly_exposure / SUM(monthly_exposure) OVER (PARTITION BY loan_grade) * 100`.
    - Rolling 3-month moving average of monthly issuance volume.

---

## 4. Data Quality & Audit Framework

The pipeline includes an enterprise Data Quality Engine that enforces 13 distinct assertions across all layers, logging results to `data/delta/audit/dq_validation_results`:

| Check Name | Target Table | Type | Severity | Validation Rule |
| :--- | :--- | :--- | :--- | :--- |
| `bronze_source_reconciliation` | `bronze_loans` | Reconciliation | CRITICAL | Raw CSV row count == Bronze row count |
| `bronze_corrupt_records_threshold` | `bronze_loans` | Data Integrity | WARNING | Corrupt CSV lines <= 1.0% |
| `silver_record_conservation_reconciliation` | `silver_loans` | Reconciliation | CRITICAL | Accepted + Quarantine == Bronze Count |
| `silver_loan_id_uniqueness` | `silver_loans` | Uniqueness | CRITICAL | Zero duplicate `loan_id` values |
| `silver_null_check_loan_id` | `silver_loans` | Null Check | CRITICAL | 0.0% nulls in `loan_id` |
| `silver_null_check_loan_amount` | `silver_loans` | Null Check | CRITICAL | 0.0% nulls in `loan_amount` |
| `silver_null_check_annual_income` | `silver_loans` | Null Check | CRITICAL | 0.0% nulls in `annual_income` |
| `silver_null_check_interest_rate_pct` | `silver_loans` | Null Check | CRITICAL | 0.0% nulls in `interest_rate_pct` |
| `silver_null_check_loan_grade` | `silver_loans` | Null Check | CRITICAL | 0.0% nulls in `loan_grade` |
| `silver_null_check_loan_status` | `silver_loans` | Null Check | CRITICAL | 0.0% nulls in `loan_status` |
| `silver_range_check_loan_amount` | `silver_loans` | Range Check | CRITICAL | Loan amounts within [$500, $100,000] |
| `silver_domain_check_loan_grade` | `silver_loans` | Domain Check | CRITICAL | Grades strictly in {A, B, C, D, E, F, G} |
| `gold_portfolio_default_rate_sanity` | `gold_analytics` | Sanity Check | WARNING | Overall default rate between 1.0% and 60.0% |
| `gold_exposure_cross_table_consistency` | `gold_analytics` | Consistency | CRITICAL | Grade exposure sum == Portfolio exposure sum ($\Delta \le \$0.05$) |

---

## 5. Performance and Reliability Best Practices

1. **Explicit Schemas Everywhere:**
   All reads and transformations avoid runtime schema inference scans over large CSV directories.
2. **Delta Lake Transaction Log & ACID Semantics:**
   Leverages Delta Lake's `_delta_log` for atomic commits, time-travel auditing, and concurrent write protection.
3. **Optimized Deduplication & Shuffle Management:**
   Window-based ranking avoids expensive self-joins. Partitions are tuned to prevent small file problems (`spark.sql.shuffle.partitions`).
4. **Liquid Clustering & Z-Ordering (Databricks Production):**
   In Azure Databricks production, Silver and Gold tables are clustered using:
   ```sql
   ALTER TABLE silver_loans CLUSTER BY (loan_grade, borrower_state);
   ```
   Or optimized via Z-Order:
   ```sql
   OPTIMIZE silver_loans ZORDER BY (loan_grade, issue_date);
   ```
5. **Cost-Conscious Cloud Execution:**
   - Single-node cluster (`Standard_D4ds_v5` or `Standard_E4ds_v5`) for dev and integration testing.
   - Autoscaling with Azure Spot instances (up to 70% cost reduction) for non-critical batch processing.
   - Auto-termination set to 15-20 minutes of inactivity.
