import pandas as pd
from pathlib import Path
from datetime import datetime


# config
VALID_ACCOUNT_STATUS = {"active", "inactive", "suspended", "closed"}
REPORT_FILE = "../deliverables/data_quality_report.txt"



# helper functions
def completeness_report(df: pd.DataFrame):
    report = {}
    total_rows = len(df)

    for col in df.columns:
        missing = df[col].isna().sum()
        percent_complete = round(((total_rows - missing) / total_rows) * 100, 2)
        report[col] = {
            "percent_complete": percent_complete,
            "missing_count": missing
        }

    return report


def detect_data_types(df: pd.DataFrame):
    return {col: str(dtype) for col, dtype in df.dtypes.items()}


def check_uniqueness(df: pd.DataFrame, column: str):
    duplicates = df[df.duplicated(subset=[column], keep=False)]
    return duplicates


def validate_dates(df: pd.DataFrame, column: str):
    invalid_rows = []
    for idx, value in df[column].items():
        try:
            pd.to_datetime(value)
        except Exception:
            invalid_rows.append((idx, value))
    return invalid_rows


def validate_numeric(df: pd.DataFrame, column: str, condition):
    invalid = df[~condition(df[column])]
    return invalid


def validate_categorical(df: pd.DataFrame, column: str, valid_set):
    invalid = df[~df[column].isin(valid_set)]
    return invalid



#main profiling function
def profile_data(csv_path: str):
    df = pd.read_csv(csv_path)

    completeness = completeness_report(df)
    dtypes = detect_data_types(df)

    issues = []

    # Uniqueness
    if "customer_id" in df.columns:
        duplicates = check_uniqueness(df, "customer_id")
        if not duplicates.empty:
            issues.append(("Duplicate customer_id", duplicates.head(5)))

    # Date validation
    if "date_of_birth" in df.columns:
        invalid_dates = validate_dates(df, "date_of_birth")
        if invalid_dates:
            issues.append(("Invalid date_of_birth values", invalid_dates[:5]))

    # Income validation
    if "income" in df.columns:
        negative_income = validate_numeric(
            df,
            "income",
            lambda x: x >= 0
        )
        if not negative_income.empty:
            issues.append(("Negative income values", negative_income.head(5)))

    # Age validation
    if "age" in df.columns:
        invalid_age = validate_numeric(
            df,
            "age",
            lambda x: (x >= 0) & (x <= 150)
        )
        if not invalid_age.empty:
            issues.append(("Invalid age values", invalid_age.head(5)))

    # Account status validation
    if "account_status" in df.columns:
        invalid_status = validate_categorical(
            df,
            "account_status",
            VALID_ACCOUNT_STATUS
        )
        if not invalid_status.empty:
            issues.append(("Invalid account_status values", invalid_status.head(5)))

    generate_report(df, completeness, dtypes, issues)



#report generation 
def generate_report(df, completeness, dtypes, issues):
    total_rows = len(df)

    critical = 0
    high = 0
    medium = 0

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("DATA QUALITY PROFILE REPORT\n")
        f.write("===========================\n\n")

        # COMPLETENESS
        f.write("COMPLETENESS:\n")
        for col, stats in completeness.items():
            f.write(
                f"- {col}: {stats['percent_complete']}% "
                f"({stats['missing_count']} missing)\n"
            )

            if stats["percent_complete"] < 90:
                high += 1
            elif stats["percent_complete"] < 100:
                medium += 1

        f.write("\nDATA TYPES:\n")
        for col, dtype in dtypes.items():
            f.write(f"- {col}: {dtype}\n")

        f.write("\nQUALITY ISSUES:\n")
        if not issues:
            f.write("No major quality issues detected.\n")
        else:
            for idx, (issue_name, examples) in enumerate(issues, 1):
                f.write(f"{idx}. {issue_name}\n")
                f.write(f"   Examples:\n{examples}\n\n")

                if "Duplicate" in issue_name:
                    critical += 1
                elif "Invalid" in issue_name or "Negative" in issue_name:
                    high += 1
                else:
                    medium += 1

        f.write("\nSEVERITY:\n")
        f.write(f"- Critical (blocks processing): {critical}\n")
        f.write(f"- High (data incorrect): {high}\n")
        f.write(f"- Medium (needs cleaning): {medium}\n")

    print(f"Report generated: {REPORT_FILE}")




if __name__ == "__main__":
    profile_data("./customers_raw.csv")
