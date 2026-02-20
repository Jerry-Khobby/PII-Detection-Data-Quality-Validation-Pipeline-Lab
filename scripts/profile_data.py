import pandas as pd
import logging

logger = logging.getLogger(__name__)

VALID_ACCOUNT_STATUS = {"active", "inactive", "suspended"}


def completeness_report(df):
    total_rows = len(df)
    report = {}
    for col in df.columns:
        missing_rows = df[df[col].isna()].index.tolist()
        missing_count = len(missing_rows)
        percent_complete = round(((total_rows - missing_count) / total_rows) * 100, 2)
        report[col] = {
            "percent_complete": percent_complete,
            "missing_count": missing_count,
            "missing_rows": missing_rows,
        }
    return report


def detect_data_types(df):
    return {col: str(dtype) for col, dtype in df.dtypes.items()}


def check_duplicates(df, column):
    dupes = df[df.duplicated(subset=[column], keep=False)].copy()
    return dupes


def validate_dates(df, column):
    mask = pd.to_datetime(df[column], errors="coerce").isna() & df[column].notna()
    return df[mask].copy()


def validate_numeric(df, column, condition):
    numeric_series = pd.to_numeric(df[column], errors="coerce")
    invalid_mask = ~condition(numeric_series)
    return df[invalid_mask].copy()


def validate_categorical(df, column, valid_set):
    invalid_mask = ~df[column].isin(valid_set) & df[column].notna()
    return df[invalid_mask].copy()


def validate_customer_id(df):
    issues = []

    if "customer_id" in df.columns:
        non_positive = df[pd.to_numeric(df["customer_id"], errors="coerce").fillna(-1) <= 0]
        if not non_positive.empty:
            issues.append(("customer_id: non-positive values", non_positive))

        dupes = check_duplicates(df, "customer_id")
        if not dupes.empty:
            issues.append(("customer_id: duplicate values", dupes))

    return issues


def validate_name(df, column):
    issues = []
    if column not in df.columns:
        return issues

    non_empty = df[column].notna()
    length_mask = non_empty & ((df[column].str.len() < 2) | (df[column].str.len() > 50))
    alpha_mask = non_empty & ~df[column].str.replace("-", "", regex=False).str.replace(" ", "", regex=False).str.isalpha()

    if length_mask.any():
        issues.append((f"{column}: length out of range (2-50)", df[length_mask].copy()))
    if alpha_mask.any():
        issues.append((f"{column}: non-alphabetic characters", df[alpha_mask].copy()))

    return issues


def validate_email(df):
    if "email" not in df.columns:
        return []
    valid_mask = df["email"].str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", na=False)
    invalid = df[~valid_mask & df["email"].notna()]
    return [("email: invalid format", invalid.copy())] if not invalid.empty else []


def validate_phone(df):
    if "phone" not in df.columns:
        return []
    digits_only = df["phone"].astype(str).str.replace(r"[\s\-().+]", "", regex=True)
    invalid_mask = ~digits_only.str.match(r"^\d{7,15}$") & df["phone"].notna()
    invalid = df[invalid_mask].copy()
    return [("phone: invalid or non-normalizable format", invalid)] if not invalid.empty else []


def validate_address(df):
    if "address" not in df.columns:
        return []
    mask = df["address"].notna() & ((df["address"].str.len() < 10) | (df["address"].str.len() > 200))
    invalid = df[mask].copy()
    return [("address: length out of range (10-200)", invalid)] if not invalid.empty else []


def validate_income(df):
    if "income" not in df.columns:
        return []
    invalid = validate_numeric(df, "income", lambda x: (x >= 0) & (x <= 10_000_000))
    return [("income: out of valid range (0-10M)", invalid)] if not invalid.empty else []


def validate_age(df):
    if "age" not in df.columns:
        return []
    invalid = validate_numeric(df, "age", lambda x: (x >= 0) & (x <= 150))
    return [("age: invalid values (0-150)", invalid)] if not invalid.empty else []


def validate_account_status(df):
    if "account_status" not in df.columns:
        return []
    invalid = validate_categorical(df, "account_status", VALID_ACCOUNT_STATUS)
    return [("account_status: invalid values", invalid)] if not invalid.empty else []


def validate_date_column(df, column):
    if column not in df.columns:
        return []
    invalid = validate_dates(df, column)
    return [(f"{column}: invalid date format", invalid)] if not invalid.empty else []


def profile_data(csv_path):
    logger.info(f"Starting data profiling: {csv_path}")
    
    df = pd.read_csv(csv_path)
    completeness = completeness_report(df)
    dtypes = detect_data_types(df)

    issues = []
    issues.extend(validate_customer_id(df))
    issues.extend(validate_name(df, "first_name"))
    issues.extend(validate_name(df, "last_name"))
    issues.extend(validate_email(df))
    issues.extend(validate_phone(df))
    issues.extend(validate_date_column(df, "date_of_birth"))
    issues.extend(validate_address(df))
    issues.extend(validate_income(df))
    issues.extend(validate_age(df))
    issues.extend(validate_account_status(df))
    issues.extend(validate_date_column(df, "created_date"))

    log_results(df, completeness, dtypes, issues)
    
    logger.info(f"Data profiling complete. Found {len(issues)} issues")
    
    return {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "completeness": completeness,
        "data_types": dtypes,
        "issues_found": len(issues),
        "issues": issues
    }


def log_results(df, completeness, dtypes, issues):
    logger.info("===== DATA QUALITY PROFILE =====")
    logger.info(f"Total rows: {len(df)} | Total columns: {len(df.columns)}")

    logger.info("----- COMPLETENESS -----")
    for col, stats in completeness.items():
        if stats["missing_count"] > 0:
            logger.info(
                f"{col}: {stats['percent_complete']}% complete | "
                f"{stats['missing_count']} missing | row indices: {stats['missing_rows']}"
            )
        else:
            logger.info(f"{col}: 100% complete")

    logger.info("----- DATA TYPES -----")
    for col, dtype in dtypes.items():
        logger.info(f"{col}: {dtype}")

    logger.info("----- ISSUES FOUND -----")
    for idx, (issue_name, df_rows) in enumerate(issues, 1):
        logger.info(f"{idx}. {issue_name} | affected rows: {len(df_rows)}")
        if not df_rows.empty:
            logger.debug(df_rows.to_string(index=True))
