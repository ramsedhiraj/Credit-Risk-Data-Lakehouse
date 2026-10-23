-- ============================================================================
-- File: gold_analytics.sql
-- Description: Spark SQL implementations of Gold Layer Credit Risk Analytical Marts.
--              Features aggregations, cohort default rate calculations,
--              sample-size safeguards, and advanced window functions.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Default Rate and Exposure by Loan Grade
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_default_rate_by_grade AS
SELECT
    loan_grade,
    COUNT(*)                                                   AS total_loans,
    SUM(is_default)                                            AS defaulted_loans,
    SUM(CASE WHEN loan_status = 'Fully Paid' THEN 1 ELSE 0 END) AS fully_paid_loans,
    SUM(CASE WHEN is_resolved = 0 THEN 1 ELSE 0 END)           AS active_loans,
    -- Cohort Default Rate: Defaults divided by resolved outcome loans (Fully Paid + Charged Off)
    ROUND(SUM(is_default) / NULLIF(SUM(is_resolved), 0) * 100, 2) AS cohort_default_rate_pct,
    -- Total Default Rate: Defaults divided by all loans in portfolio
    ROUND(SUM(is_default) / COUNT(*) * 100, 2)                 AS total_default_rate_pct,
    ROUND(SUM(loan_amount), 2)                                 AS total_loan_exposure,
    ROUND(AVG(loan_amount), 2)                                 AS avg_loan_amount,
    ROUND(AVG(interest_rate_pct), 2)                           AS avg_interest_rate_pct,
    ROUND(SUM(gross_recoveries), 2)                            AS total_gross_recoveries
FROM silver_loans
GROUP BY loan_grade
ORDER BY loan_grade ASC;

-- ----------------------------------------------------------------------------
-- 2. Default Rate by State (with Minimum Sample-Size Safeguard)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_default_rate_by_state AS
WITH state_aggregates AS (
    SELECT
        borrower_state,
        COUNT(*)                                       AS total_loans,
        SUM(is_default)                                AS defaulted_loans,
        ROUND(SUM(is_default) / COUNT(*) * 100, 2)     AS default_rate_pct,
        ROUND(SUM(loan_amount), 2)                     AS total_loan_exposure,
        ROUND(AVG(annual_income), 2)                   AS avg_annual_income,
        -- Safeguard: flag states with >= 10 loans as statistically reliable
        CASE WHEN COUNT(*) >= 10 THEN TRUE ELSE FALSE END AS has_sufficient_sample_size
    FROM silver_loans
    GROUP BY borrower_state
)
SELECT
    borrower_state,
    total_loans,
    defaulted_loans,
    default_rate_pct,
    total_loan_exposure,
    avg_annual_income,
    has_sufficient_sample_size,
    -- Window function: Rank states by default rate descending among qualified states
    CASE 
        WHEN has_sufficient_sample_size THEN 
            DENSE_RANK() OVER (
                PARTITION BY has_sufficient_sample_size 
                ORDER BY default_rate_pct DESC, total_loans DESC
            )
        ELSE NULL
    END AS state_risk_rank
FROM state_aggregates
ORDER BY has_sufficient_sample_size DESC, state_risk_rank ASC NULLS LAST;

-- ----------------------------------------------------------------------------
-- 3. Default Rate by Income Band
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_default_rate_by_income_band AS
SELECT
    income_band,
    COUNT(*)                                       AS total_loans,
    SUM(is_default)                                AS defaulted_loans,
    ROUND(SUM(is_default) / COUNT(*) * 100, 2)     AS default_rate_pct,
    ROUND(SUM(loan_amount), 2)                     AS total_loan_exposure,
    ROUND(AVG(loan_amount), 2)                     AS avg_loan_amount,
    ROUND(AVG(debt_to_income_ratio), 2)            AS avg_debt_to_income_ratio
FROM silver_loans
GROUP BY income_band
ORDER BY income_band ASC;

-- ----------------------------------------------------------------------------
-- 4. Executive Portfolio Summary
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_loan_portfolio_summary AS
SELECT
    COUNT(*)                                                   AS total_portfolio_loans,
    ROUND(SUM(loan_amount), 2)                                 AS total_exposure_amount,
    ROUND(AVG(loan_amount), 2)                                 AS avg_loan_amount,
    ROUND(AVG(interest_rate_pct), 2)                           AS avg_interest_rate_pct,
    ROUND(SUM(is_default) / COUNT(*) * 100, 2)                 AS overall_default_rate_pct,
    ROUND(SUM(is_default) / NULLIF(SUM(is_resolved), 0) * 100, 2) AS resolved_cohort_default_rate_pct,
    SUM(is_default)                                            AS total_defaulted_loans,
    SUM(CASE WHEN loan_status = 'Fully Paid' THEN 1 ELSE 0 END) AS total_fully_paid_loans,
    SUM(CASE WHEN is_resolved = 0 THEN 1 ELSE 0 END)           AS total_active_loans,
    ROUND(SUM(total_payment_received), 2)                      AS total_payments_received,
    ROUND(SUM(gross_recoveries), 2)                            AS total_gross_recoveries
FROM silver_loans;

-- ----------------------------------------------------------------------------
-- 5. Window Function Risk Analytics (Running Totals, Cohort Ranks & Contributions)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_loan_window_analytics AS
WITH monthly_grade_cohorts AS (
    SELECT
        loan_grade,
        DATE_TRUNC('month', issue_date) AS issue_month,
        COUNT(*)                        AS monthly_loans_issued,
        ROUND(SUM(loan_amount), 2)      AS monthly_exposure_issued,
        SUM(is_default)                 AS monthly_defaults
    FROM silver_loans
    WHERE issue_date IS NOT NULL
    GROUP BY loan_grade, DATE_TRUNC('month', issue_date)
)
SELECT
    loan_grade,
    issue_month,
    monthly_loans_issued,
    monthly_exposure_issued,
    monthly_defaults,

    -- Window 1: Running cumulative loan count per grade over chronological issue dates
    SUM(monthly_loans_issued) OVER (
        PARTITION BY loan_grade 
        ORDER BY issue_month 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_loans_issued,

    -- Window 2: Running cumulative exposure ($) per grade over time
    ROUND(SUM(monthly_exposure_issued) OVER (
        PARTITION BY loan_grade 
        ORDER BY issue_month 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS cumulative_exposure_issued,

    -- Window 3: Total lifetime exposure across all cohorts for the grade
    ROUND(SUM(monthly_exposure_issued) OVER (
        PARTITION BY loan_grade
    ), 2) AS grade_lifetime_exposure,

    -- Window 4: Percentage of grade's total lifetime exposure contributed by this specific month
    ROUND(
        monthly_exposure_issued / SUM(monthly_exposure_issued) OVER (PARTITION BY loan_grade) * 100,
        2
    ) AS monthly_exposure_pct_of_grade,

    -- Window 5: Dense ranking of issuance volume months within each grade
    DENSE_RANK() OVER (
        PARTITION BY loan_grade 
        ORDER BY monthly_loans_issued DESC
    ) AS volume_rank_within_grade,

    -- Window 6: 3-Month Moving Average of monthly issued loan amount
    ROUND(AVG(monthly_exposure_issued) OVER (
        PARTITION BY loan_grade 
        ORDER BY issue_month 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_3m_exposure

FROM monthly_grade_cohorts
ORDER BY loan_grade ASC, issue_month ASC;
