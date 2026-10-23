-- ============================================================================
-- File: silver_cleaning.sql
-- Description: Spark SQL implementation of Silver Layer Transformations.
--              Demonstrates CTEs, regex cleaning, window deduplication,
--              business validation rules, and idempotent MERGE INTO logic.
-- ============================================================================

-- Step 1: Incremental or Batch Deduplication & Cleaning View
CREATE OR REPLACE TEMPORARY VIEW v_silver_loans_cleaned AS
WITH bronze_typed AS (
    SELECT
        TRIM(id)                                                   AS loan_id,
        TRIM(member_id)                                            AS borrower_id,
        CAST(REGEXP_REPLACE(TRIM(loan_amnt), '[%,\\$ ]', '') AS DOUBLE) AS loan_amount,
        CAST(REGEXP_REPLACE(TRIM(funded_amnt), '[%,\\$ ]', '') AS DOUBLE) AS funded_amount,
        CAST(REGEXP_EXTRACT(term, '(\\d+)', 1) AS INT)             AS term_months,
        CAST(REGEXP_REPLACE(TRIM(int_rate), '[%,\\$ ]', '') AS DOUBLE) AS interest_rate_pct,
        CAST(REGEXP_REPLACE(TRIM(installment), '[%,\\$ ]', '') AS DOUBLE) AS installment_amount,
        UPPER(TRIM(grade))                                         AS loan_grade,
        UPPER(TRIM(sub_grade))                                     AS loan_subgrade,
        NULLIF(TRIM(emp_title), '')                                AS employment_title,
        CASE
            WHEN emp_length LIKE '%10+%' THEN 10
            WHEN emp_length LIKE '%< 1%' THEN 0
            WHEN emp_length IN ('n/a', 'NA', '') THEN NULL
            ELSE CAST(REGEXP_EXTRACT(emp_length, '(\\d+)', 1) AS INT)
        END                                                        AS employment_length_years,
        UPPER(TRIM(home_ownership))                                AS home_ownership_status,
        CAST(REGEXP_REPLACE(TRIM(annual_inc), '[%,\\$ ]', '') AS DOUBLE) AS annual_income,
        TRIM(verification_status)                                  AS income_verification_status,
        TO_DATE(CONCAT('01-', TRIM(issue_d)), 'dd-MMM-yyyy')       AS issue_date,
        TRIM(loan_status)                                          AS loan_status,
        LOWER(TRIM(purpose))                                       AS loan_purpose,
        UPPER(TRIM(addr_state))                                    AS borrower_state,
        CAST(REGEXP_REPLACE(TRIM(dti), '[%,\\$ ]', '') AS DOUBLE)  AS debt_to_income_ratio,
        CAST(REGEXP_REPLACE(TRIM(delinq_2yrs), '[%,\\$ ]', '') AS INT) AS delinquency_2yrs_count,
        TRIM(earliest_cr_line)                                     AS earliest_credit_line_date,
        CAST(REGEXP_REPLACE(TRIM(open_acc), '[%,\\$ ]', '') AS INT) AS open_accounts_count,
        CAST(REGEXP_REPLACE(TRIM(pub_rec), '[%,\\$ ]', '') AS INT)  AS derogatory_records_count,
        CAST(REGEXP_REPLACE(TRIM(revol_bal), '[%,\\$ ]', '') AS DOUBLE) AS revolving_balance,
        CAST(REGEXP_REPLACE(TRIM(revol_util), '[%,\\$ ]', '') AS DOUBLE) AS revolving_utilization_pct,
        CAST(REGEXP_REPLACE(TRIM(total_acc), '[%,\\$ ]', '') AS INT) AS total_accounts_count,
        CAST(REGEXP_REPLACE(TRIM(total_pymnt), '[%,\\$ ]', '') AS DOUBLE) AS total_payment_received,
        CAST(REGEXP_REPLACE(TRIM(recoveries), '[%,\\$ ]', '') AS DOUBLE) AS gross_recoveries,
        _ingestion_timestamp,
        _source_file,
        _batch_id,
        _corrupt_record
    FROM bronze_loans
),
bronze_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id 
            ORDER BY _ingestion_timestamp DESC
        ) AS row_num
    FROM bronze_typed
),
silver_validated AS (
    SELECT
        *,
        -- Credit Risk Classification Flags
        CASE 
            WHEN loan_status IN ('Charged Off', 'Default') THEN 1 
            ELSE 0 
        END AS is_default,
        CASE 
            WHEN loan_status IN ('Fully Paid', 'Charged Off', 'Default') THEN 1 
            ELSE 0 
        END AS is_resolved,
        -- Income Band Bucketing
        CASE
            WHEN annual_income < 30000 THEN '1. Low (<$30k)'
            WHEN annual_income < 60000 THEN '2. Lower-Middle ($30k-$60k)'
            WHEN annual_income < 100000 THEN '3. Upper-Middle ($60k-$100k)'
            WHEN annual_income < 150000 THEN '4. High ($100k-$150k)'
            ELSE '5. Very High (>$150k)'
        END AS income_band,
        CURRENT_TIMESTAMP() AS _silver_processed_at
    FROM bronze_ranked
    WHERE row_num = 1
      AND loan_id IS NOT NULL AND loan_id != ''
      AND loan_amount >= 500.0 AND loan_amount <= 100000.0
      AND annual_income >= 1000.0 AND annual_income <= 10000000.0
      AND interest_rate_pct >= 0.0 AND interest_rate_pct <= 100.0
      AND loan_grade IN ('A', 'B', 'C', 'D', 'E', 'F', 'G')
      AND loan_status IS NOT NULL AND loan_status != ''
      AND _corrupt_record IS NULL
)
SELECT * EXCEPT(row_num, _corrupt_record)
FROM silver_validated;

-- Step 2: Idempotent MERGE into Target Silver Delta Table
MERGE INTO silver_loans AS target
USING v_silver_loans_cleaned AS source
ON target.loan_id = source.loan_id
WHEN MATCHED AND source._ingestion_timestamp > target._ingestion_timestamp THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;
