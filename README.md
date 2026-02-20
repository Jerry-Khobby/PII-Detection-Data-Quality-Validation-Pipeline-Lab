# PII Detection & Data Quality Validation Pipeline Lab

Data Engineering Mini Project

---

## Overview

This project implements an end-to-end data engineering pipeline to profile, validate, clean, detect, and mask Personally Identifiable Information (PII) in a customer dataset collected from multiple sources.

The objective is to simulate real-world data engineering work in a fintech environment where:

* Data arrives messy and inconsistent
* Sensitive information must be protected
* Data quality must be enforced before analytics usage
* Governance and compliance requirements must be considered

The pipeline performs structured profiling, validation, remediation, masking, and reporting in a production-style workflow.

---

## Dataset Description

Input file: `customers_raw.csv`

The dataset contains 10 customer records with the following columns:

* customer_id
* first_name
* last_name
* email
* phone
* date_of_birth
* address
* income
* account_status
* created_date

The data contains:

* Missing values
* Incorrect data types
* Invalid formats (dates, phone numbers)
* Invalid categorical values
* Exposed PII

---

## Expected Schema & Validation Rules

| Column         | Type    | Rules                             |
| -------------- | ------- | --------------------------------- |
| customer_id    | Integer | Unique, positive                  |
| first_name     | String  | Non-empty, 2–50 chars, alphabetic |
| last_name      | String  | Non-empty, 2–50 chars, alphabetic |
| email          | String  | Valid email format                |
| phone          | String  | Valid phone format                |
| date_of_birth  | Date    | Valid date, YYYY-MM-DD            |
| address        | String  | 10–200 characters                 |
| income         | Float   | 0 ≤ income ≤ 10,000,000           |
| account_status | String  | active, inactive, suspended       |
| created_date   | Date    | Valid date, YYYY-MM-DD            |

Validation is implemented using Pydantic models and custom logic.

---

## Project Structure

```
project/
│
├── main.py                  # Pipeline orchestrator
│
├── data/
│   ├── customers_raw.csv
│   ├── customers_cleaned.csv
│   └── customers_masked.csv
│
├── scripts/                 # Modular processing logic
│   ├── clean_data.py
│   ├── validator.py
│   ├── profile_data.py
│   ├── detect_pii.py
│   ├── mask_pii.py
│   └── logging.py
│
├── deliverables/            # All generated reports
│   ├── pipeline_execution_report.txt
│   ├── data_quality_report.txt
│   ├── validation_results.txt
│   ├── pii_detection_report.txt
│   ├── cleaning_log.txt
│   └── masking_sample.txt
│
└── logs/
    └── pipeline.log
```

---

## Part 1: Exploratory Data Quality Analysis

Deliverable: `data_quality_report.txt`

Findings:

Completeness Issues:

* Missing first_name (1 row)
* Missing last_name (1 row)
* Missing address (1 row)
* Missing income (1 row)
* Missing account_status (1 row)
* Missing created_date (1 row)

Data Type Issues:

* date_of_birth stored as STRING (should be DATE)
* income stored as STRING (should be FLOAT)
* created_date stored as STRING (should be DATE)

Severity Assessment:

* Critical: 0 (no duplicate or invalid customer_id)
* High: 3 (type mismatches)
* Medium: 6 (missing values)

Impact:
Without cleaning, 60% of records would fail strict schema validation.

---

## Part 2: PII Detection

Deliverable: `pii_detection_report.txt`

Detected PII:

* Email addresses: 100%
* Phone numbers: 100%
* Physical addresses: 90%
* Dates of birth: 100%
* Full identity linkage (first + last + email): 80%

Risk Classification:

High Sensitivity:

* Names
* Emails
* Phone numbers
* Addresses
* Dates of birth

Medium Sensitivity:

* Income

Risk Assessment:

The dataset is classified as HIGH RISK because:

* 100% contact exposure
* 80% full identity linkage
* Multiple strong identifiers in single records

Potential damage if breached:

* Phishing
* SIM swap attacks
* Identity theft
* Social engineering
* Account takeover

---

## Part 3: Validation

Deliverable: `validation_results.txt`

Before Cleaning:

* Total rows: 10
* Passed: 4
* Failed: 6

Major failures:

* Invalid income values
* Invalid account_status values
* Incorrect date formats
* Missing required fields
* Address length violations

After Cleaning:

* Rows retained: 9
* Passed validation: 9
* Failed: 0
* Status: PASS

Validation improvement:
Failures reduced from 6 to 0 (after removal of 1 structurally invalid record).

---

## Part 4: Data Cleaning

Deliverable: `cleaning_log.txt`

Cleaning Strategy:

Only `customer_id` is treated as a critical field.
All other missing fields are filled using safe placeholder values to maximize data retention.

Normalization Performed:

* Trimmed whitespace
* Converted names to Title Case
* Standardized phone format to XXX-XXX-XXXX
* Converted dates to YYYY-MM-DD
* Converted numeric types properly

Missing Value Handling:

* first_name → "Unknown"
* last_name → "Unknown"
* email → [noemail@placeholder.com](mailto:noemail@placeholder.com)
* phone → 000-000-0000
* address → Address Not Provided
* income → 0.0
* account_status → inactive
* date_of_birth → 1900-01-01
* created_date → current date

Result:

* Before cleaning: 6 failed validations
* After cleaning: 0 failed
* Data quality significantly improved

---

## Part 5: PII Masking

Deliverable: `masked_sample.txt`

Masking Rules:

* Names → J***
* Emails → j***@domain.com
* Phone → ***-***-4567
* Address → [MASKED ADDRESS]
* DOB → 1985-**-**

Result:

* Structure preserved (10 columns)
* Business data intact
* PII fully masked
* Safe for analytics team usage

Trade-off:

Masking reduces operational utility (cannot contact customers), but ensures privacy compliance and reduces breach impact.

---

## Part 6: End-to-End Pipeline

Deliverable: `pipeline_execution_report.txt`

The pipeline orchestrates:

1. Load raw data
2. Clean and normalize
3. Validate against schema
4. Detect PII
5. Mask PII
6. Save outputs and reports

Pipeline Results:

* Input rows: 10
* Output rows: 9
* Validation: PASS
* PII records detected: 45 fields across 9 rows
* Status: SUCCESS

The system runs automatically without manual intervention.

Production Readiness Features:

* Modular architecture
* Centralized logging
* Structured validation
* Error handling
* Automated reporting
* Deterministic output files

---

## Part 7: Governance & Reflection

Deliverable: `reflection.md`

Key Themes Addressed:

* Top 5 data quality problems
* PII exposure risks
* Masking trade-offs
* Validation coverage gaps
* Production failure handling
* Operational scheduling strategy
* Lessons learned

Operational Considerations:

In production, this pipeline would run:

* Daily batch job (recommended)
* On ingestion trigger
* With alerting on validation failure

If validation fails:

* Pipeline stops
* Failed rows logged
* Alert sent to data engineering team
* Flagged dataset stored for review

---

## How to Run

1. Install dependencies:

   ```
   pip install pandas pydantic
   ```

2. Place `customers_raw.csv` in the project directory.

3. Run:

   ```
   python main.py
   ```

Outputs generated automatically:

* customers_cleaned.csv
* customers_masked.csv
* data_quality_report.txt
* validation_results.txt
* pii_detection_report.txt
* cleaning_log.txt
* masked_sample.txt
* pipeline_execution_report.txt

---

## Key Concepts Demonstrated

* Data profiling and completeness analysis
* Schema validation with Pydantic
* Data normalization and remediation
* Regex-based PII detection
* Structured PII masking
* ETL pipeline orchestration
* Logging and operational transparency
* Governance and compliance thinking

---

## Final Status

All required deliverables completed.

The dataset has been:

* Profiled
* Validated
* Cleaned
* Masked
* Documented
* Automated

This project simulates real-world data engineering work in a regulated fintech environment and demonstrates production-oriented pipeline design.
