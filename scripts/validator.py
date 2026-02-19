from pydantic import BaseModel, EmailStr, field_validator, ValidationError
from typing import Literal
from datetime import date
import pandas as pd
import re
import logging

# Configure logging
logging.basicConfig(
    filename="../logs/validation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Schema Model
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


# Validation logic
def validate_dataset(csv_path):
    df = pd.read_csv(csv_path)
    
    df.columns = df.columns.str.strip()
    
    for col in df.select_dtypes(include=["object"]).columns:
      df[col] = df[col].astype(str).str.strip()
    
    failed_rows = []
    seen_ids = set()
    
    for index, row in df.iterrows():
        try:
            # Check uniqueness
            if row["customer_id"] in seen_ids:
                raise ValueError("Duplicate customer_id")
            seen_ids.add(row["customer_id"])
            
            # Validate row
            Customer(**row.to_dict())
            
        except (ValidationError, ValueError) as e:
            failed_rows.append({
                "row_index": index,
                "customer_id": row.get("customer_id"),
                "error": str(e)
            })
    
    # Log results
    if failed_rows:
        logging.error("DATA VALIDATION FAILED")
        for failure in failed_rows:
            logging.error(failure)
    else:
        logging.info("All rows passed validation")

    return failed_rows


# run validation
if __name__ == "__main__":
    failed = validate_dataset("./customers_raw.csv")
    
    if failed:
        print("\nFailed rows summary:")
        for f in failed:
            print(f"Row {f['row_index']} (ID: {f['customer_id']}): {f['error']}")
