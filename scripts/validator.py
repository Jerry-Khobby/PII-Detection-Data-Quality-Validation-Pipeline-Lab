from pydantic import BaseModel, EmailStr, field_validator, ValidationError
from typing import Literal
from datetime import date
import pandas as pd
import re
import logging

logger = logging.getLogger(__name__)


class Customer(BaseModel):
    customer_id: int
    first_name: str
    last_name: str
    email: EmailStr
    phone: str
    date_of_birth: date
    address: str
    income: float
    account_status: Literal["active", "inactive", "suspended"]
    created_date: date

    @field_validator("customer_id")
    def id_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("customer_id must be positive")
        return v

    @field_validator("first_name", "last_name")
    def name_rules(cls, v):
        if not v:
            raise ValueError("Name cannot be empty")
        if not re.match(r"^[A-Za-z]{2,50}$", v):
            raise ValueError("Name must be 2–50 alphabetic characters")
        return v

    @field_validator("phone")
    def phone_length(cls, v):
        if len(str(v)) < 7 or len(str(v)) > 20:
            raise ValueError("Phone length invalid")
        return v

    @field_validator("address")
    def address_rules(cls, v):
        if not v or len(v) < 10 or len(v) > 200:
            raise ValueError("Address must be 10–200 characters")
        return v

    @field_validator("income")
    def income_rules(cls, v):
        if v < 0:
            raise ValueError("Income cannot be negative")
        if v > 10_000_000:
            raise ValueError("Income exceeds limit")
        return v


def validate_dataset(csv_path):
    logger.info(f"Starting dataset validation: {csv_path}")
    
    df = pd.read_csv(csv_path)

    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    failed_rows = []
    failures_by_column = {}
    seen_ids = set()

    for index, row in df.iterrows():
        row_dict = row.to_dict()

        try:
            if row_dict["customer_id"] in seen_ids:
                raise ValueError("Duplicate customer_id")
            seen_ids.add(row_dict["customer_id"])

            Customer(**row_dict)

        except (ValidationError, ValueError) as e:
            error_str = str(e)

            column_name = "unknown"
            if hasattr(e, "errors"):
                try:
                    column_name = e.errors()[0]["loc"][0]
                except:
                    pass

            failure_record = {
                "row_index": index,
                "customer_id": row_dict.get("customer_id"),
                "column": column_name,
                "error": error_str
            }

            failed_rows.append(failure_record)

            if column_name not in failures_by_column:
                failures_by_column[column_name] = []
            failures_by_column[column_name].append(failure_record)
            
            logger.warning(f"Row {index} validation failed: {error_str}")

    passed_count = len(df) - len(failed_rows)
    logger.info(f"Validation complete: {passed_count}/{len(df)} rows passed")

    return {
        "total_rows": len(df),
        "passed_count": passed_count,
        "failed_count": len(failed_rows),
        "failed_rows": failed_rows,
        "failures_by_column": failures_by_column,
        "passed": len(failed_rows) == 0
    }
