"""
Script: generate_sample_data.py
Description: Generates a realistic, schema-accurate synthetic Lending Club loan dataset
             for local development, CI/CD testing, and Databricks demonstration.
             Includes both valid records and controlled anomalies to validate Silver quarantine.
"""

import csv
import os
import random
from datetime import datetime

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "loan_data_sample.csv")

GRADES = ["A", "B", "C", "D", "E", "F", "G"]
GRADE_PROBS = [0.20, 0.28, 0.25, 0.14, 0.08, 0.03, 0.02]

GRADE_INT_RATES = {
    "A": (5.32, 8.99),
    "B": (9.16, 12.69),
    "C": (12.79, 16.29),
    "D": (16.49, 20.99),
    "E": (21.18, 25.49),
    "F": (25.89, 29.99),
    "G": (30.12, 35.50),
}

GRADE_DEFAULT_PROBS = {
    "A": 0.06,
    "B": 0.12,
    "C": 0.18,
    "D": 0.26,
    "E": 0.35,
    "F": 0.45,
    "G": 0.55,
}

STATES = [
    "CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "NJ",
    "VA", "WA", "AZ", "CO", "MI", "MA", "MD", "MO", "NV", "OR"
]

HOME_OWNERSHIPS = ["MORTGAGE", "RENT", "OWN", "OTHER"]
HOME_PROBS = [0.48, 0.40, 0.11, 0.01]

VERIFICATION_STATUSES = ["Verified", "Source Verified", "Not Verified"]
PURPOSES = [
    "debt_consolidation", "credit_card", "home_improvement", "major_purchase",
    "small_business", "medical", "car", "moving", "house", "vacation"
]
TERMS = [" 36 months", " 60 months"]
EMP_LENGTHS = [
    "< 1 year", "1 year", "2 years", "3 years", "4 years", "5 years",
    "6 years", "7 years", "8 years", "9 years", "10+ years", "n/a"
]
EMP_TITLES = [
    "Software Engineer", "Registered Nurse", "Teacher", "Manager", "Sales Associate",
    "Director", "Project Manager", "Accountant", "Mechanic", "Driver", "Administrative Assistant", None
]

ISSUE_DATES = [
    "Jan-2016", "Feb-2016", "Mar-2016", "Apr-2016", "May-2016", "Jun-2016",
    "Jul-2016", "Aug-2016", "Sep-2016", "Oct-2016", "Nov-2016", "Dec-2016",
    "Jan-2017", "Feb-2017", "Mar-2017", "Apr-2017", "May-2017", "Jun-2017",
    "Jul-2017", "Aug-2017", "Sep-2017", "Oct-2017", "Nov-2017", "Dec-2017"
]

FIELDNAMES = [
    "id", "member_id", "loan_amnt", "funded_amnt", "term", "int_rate",
    "installment", "grade", "sub_grade", "emp_title", "emp_length",
    "home_ownership", "annual_inc", "verification_status", "issue_d",
    "loan_status", "purpose", "addr_state", "dti", "delinq_2yrs",
    "earliest_cr_line", "open_acc", "pub_rec", "revol_bal", "revol_util",
    "total_acc", "total_pymnt", "recoveries"
]


def generate_valid_record(loan_id: int):
    grade = random.choices(GRADES, weights=GRADE_PROBS, k=1)[0]
    sub_num = random.randint(1, 5)
    sub_grade = f"{grade}{sub_num}"

    rate_low, rate_high = GRADE_INT_RATES[grade]
    int_rate_num = round(random.uniform(rate_low, rate_high), 2)
    # alternate between numeric and percentage string to test parsing
    int_rate_str = f"{int_rate_num}%" if random.random() < 0.5 else str(int_rate_num)

    loan_amnt = round(random.uniform(2000, 35000), -2)
    funded_amnt = loan_amnt
    term = random.choice(TERMS)
    num_months = 36 if "36" in term else 60
    r = (int_rate_num / 100.0) / 12.0
    installment = round((loan_amnt * r) / (1 - (1 + r) ** (-num_months)), 2)

    annual_inc = round(random.triangular(25000, 180000, 65000), -2)
    home_ownership = random.choices(HOME_OWNERSHIPS, weights=HOME_PROBS, k=1)[0]
    verification_status = random.choice(VERIFICATION_STATUSES)
    issue_d = random.choice(ISSUE_DATES)
    purpose = random.choice(PURPOSES)
    state = random.choice(STATES)
    dti = round(random.uniform(5.0, 38.0), 2)
    delinq_2yrs = random.choices([0, 1, 2, 3], weights=[0.82, 0.12, 0.04, 0.02], k=1)[0]
    open_acc = random.randint(3, 24)
    pub_rec = random.choices([0, 1, 2], weights=[0.88, 0.10, 0.02], k=1)[0]
    revol_bal = round(random.uniform(500, 45000), 2)
    revol_util = f"{round(random.uniform(5.0, 95.0), 1)}%"
    total_acc = open_acc + random.randint(2, 20)

    # Outcome logic
    default_prob = GRADE_DEFAULT_PROBS[grade]
    roll = random.random()
    if roll < default_prob:
        loan_status = random.choices(["Charged Off", "Default"], weights=[0.9, 0.1], k=1)[0]
        # defaulted loans have lower payments and maybe some recovery
        payments_made = round(random.uniform(0.1, 0.5) * installment * num_months, 2)
        recoveries = round(random.uniform(0, 1500), 2) if random.random() < 0.4 else 0.0
    elif roll < 0.70:
        loan_status = "Fully Paid"
        payments_made = round(installment * num_months, 2)
        recoveries = 0.0
    else:
        # active loans
        loan_status = random.choices(
            ["Current", "In Grace Period", "Late (31-120 days)", "Late (16-30 days)"],
            weights=[0.85, 0.07, 0.05, 0.03],
            k=1
        )[0]
        payments_made = round(random.uniform(0.2, 0.8) * installment * num_months, 2)
        recoveries = 0.0

    return {
        "id": str(loan_id),
        "member_id": str(loan_id + 500000),
        "loan_amnt": str(loan_amnt),
        "funded_amnt": str(funded_amnt),
        "term": term,
        "int_rate": int_rate_str,
        "installment": str(installment),
        "grade": grade,
        "sub_grade": sub_grade,
        "emp_title": random.choice(EMP_TITLES) or "",
        "emp_length": random.choice(EMP_LENGTHS),
        "home_ownership": home_ownership,
        "annual_inc": str(annual_inc),
        "verification_status": verification_status,
        "issue_d": issue_d,
        "loan_status": loan_status,
        "purpose": purpose,
        "addr_state": state,
        "dti": str(dti),
        "delinq_2yrs": str(delinq_2yrs),
        "earliest_cr_line": f"Jan-{random.randint(1990, 2010)}",
        "open_acc": str(open_acc),
        "pub_rec": str(pub_rec),
        "revol_bal": str(revol_bal),
        "revol_util": revol_util,
        "total_acc": str(total_acc),
        "total_pymnt": str(payments_made),
        "recoveries": str(recoveries),
    }


def main():
    records = []
    base_id = 10001
    total_valid = 960

    print("Generating base valid records...")
    for i in range(total_valid):
        rec = generate_valid_record(base_id + i)
        records.append(rec)

    print("Injecting controlled duplicate records (for Silver deduplication testing)...")
    # Take 10 records and duplicate them with slightly different values or identical IDs
    for dup_idx in range(10):
        src_rec = dict(records[dup_idx * 5])
        # Duplicate record with exact same id
        records.append(src_rec)

    print("Injecting controlled anomaly records (for Silver quarantine testing)...")
    anomaly_id_start = base_id + total_valid + 100

    # 1. Negative or zero loan amount
    for i in range(5):
        bad_rec = generate_valid_record(anomaly_id_start + i)
        bad_rec["loan_amnt"] = "-5000" if i % 2 == 0 else "0"
        records.append(bad_rec)

    # 2. Negative or zero annual income
    for i in range(5):
        bad_rec = generate_valid_record(anomaly_id_start + 10 + i)
        bad_rec["annual_inc"] = "-10000" if i % 2 == 0 else "0"
        records.append(bad_rec)

    # 3. Invalid grade (e.g. 'Z', '99')
    for i in range(5):
        bad_rec = generate_valid_record(anomaly_id_start + 20 + i)
        bad_rec["grade"] = "Z" if i % 2 == 0 else "99"
        records.append(bad_rec)

    # 4. Out of range interest rate (> 100%)
    for i in range(5):
        bad_rec = generate_valid_record(anomaly_id_start + 30 + i)
        bad_rec["int_rate"] = "150.0%"
        records.append(bad_rec)

    # 5. Null / empty loan ID
    for i in range(5):
        bad_rec = generate_valid_record(anomaly_id_start + 40 + i)
        bad_rec["id"] = ""
        records.append(bad_rec)

    # Shuffle to ensure realistic ordering
    random.shuffle(records)

    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)

    print(f"Generated {len(records)} sample loan records into: {OUTPUT_FILE}")
    print(f"  - Valid base records: {total_valid}")
    print(f"  - Duplicate records: 10")
    print(f"  - Anomaly records (invalid amount, income, grade, int_rate, missing id): 20")
    print(f"  - Total output rows: {len(records)}")


if __name__ == "__main__":
    main()
