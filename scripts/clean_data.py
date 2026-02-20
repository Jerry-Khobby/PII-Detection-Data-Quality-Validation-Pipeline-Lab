import pandas as pd 
import re 
from datetime import datetime, date
from scripts.validator import Customer
from pydantic import ValidationError
import logging 
import os 




logger = logging.getLogger(__name__)
def normalize_phone(phone):
    """Normalize phone to XXX-XXX-XXXX format"""
    if pd.isna(phone):
        return None
    digits = re.sub(r'\D', "", str(phone))
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    logger.warning(f"Invalid phone format: {phone}")
    return None 


def clean_dataset(input_path, output_path):
    """
    Missing Value Strategy:
    
    DELETE (row excluded from output):
    - customer_id: Primary key, absolutely required for customer tracking
    
    FILL WITH PLACEHOLDER:
    - first_name: Fill with "Unknown" (can be updated later)
    - last_name: Fill with "Unknown" (can be updated later)
    - email: Fill with "noemail@placeholder.com" (can be updated later)
    - phone: Fill with "000-000-0000" (indicates missing)
    - address: Fill with "Address Not Provided"
    - income: Fill with 0.0 (neutral value)
    - account_status: Fill with "inactive" (safe default)
    - date_of_birth: Fill with 1900-01-01 (placeholder date)
    - created_date: Fill with current date (reasonable default)
    
    Rationale: 
    - customer_id is the only truly critical field (primary key)
    - All other fields can be collected/updated later
    - Maximizes data retention while maintaining referential integrity
    """
    logger.info(f"Starting dataset cleaning: {input_path}")
    logger.info("Strategy: Delete only rows missing customer_id, fill all other fields")
    
    try:
        df = pd.read_csv(input_path)
        logger.info(f"Loaded {len(df)} rows from {input_path}")
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        raise
    
    initial_count = len(df)
    
    # Trim whitespace 
    df.columns = df.columns.str.strip()
    logger.info("Trimmed column names")
    
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].astype(str).str.strip()
    logger.info("Trimmed whitespace from object columns")
    
    # DELETE: Remove rows with missing customer_id only
    before = len(df)
    df = df[df["customer_id"].notna() & (df["customer_id"] != "") & (df["customer_id"] != "nan")]
    deleted = before - len(df)
    if deleted > 0:
        logger.warning(f"Deleted {deleted} rows with missing customer_id")
    
    logger.info(f"After deleting rows with missing customer_id: {len(df)}/{initial_count} rows remain")
    
    # Fix numeric types 
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce")
    df["income"] = pd.to_numeric(df["income"], errors="coerce")
    
    # Delete rows where customer_id conversion failed
    before = len(df)
    df = df[df["customer_id"].notna()]
    deleted = before - len(df)
    if deleted > 0:
        logger.warning(f"Deleted {deleted} rows with invalid customer_id")
    
    logger.info("Converted customer_id and income to numeric")
    
    # FILL: Replace missing names with "Unknown"
    first_name_missing = (df["first_name"].isna() | (df["first_name"] == "") | (df["first_name"] == "nan")).sum()
    df["first_name"] = df["first_name"].replace(["", "nan"], "Unknown")
    df["first_name"] = df["first_name"].fillna("Unknown")
    if first_name_missing > 0:
        logger.info(f"Filled {first_name_missing} missing first_name with 'Unknown'")
    
    last_name_missing = (df["last_name"].isna() | (df["last_name"] == "") | (df["last_name"] == "nan")).sum()
    df["last_name"] = df["last_name"].replace(["", "nan"], "Unknown")
    df["last_name"] = df["last_name"].fillna("Unknown")
    if last_name_missing > 0:
        logger.info(f"Filled {last_name_missing} missing last_name with 'Unknown'")
    
    # FILL: Replace missing email with placeholder
    email_missing = (df["email"].isna() | (df["email"] == "") | (df["email"] == "nan")).sum()
    df["email"] = df["email"].replace(["", "nan"], "noemail@placeholder.com")
    df["email"] = df["email"].fillna("noemail@placeholder.com")
    if email_missing > 0:
        logger.info(f"Filled {email_missing} missing email with 'noemail@placeholder.com'")
    
    # Normalize names to title case (only for non-placeholder values)
    df.loc[df["first_name"] != "Unknown", "first_name"] = df.loc[df["first_name"] != "Unknown", "first_name"].str.lower().str.title()
    df.loc[df["last_name"] != "Unknown", "last_name"] = df.loc[df["last_name"] != "Unknown", "last_name"].str.lower().str.title()
    logger.info("Normalized first_name and last_name to title case")
    
    # Normalize phone to XXX-XXX-XXXX format
    df["phone"] = df["phone"].apply(normalize_phone)
    
    # FILL: Replace missing phone with placeholder
    phone_missing = df["phone"].isna().sum()
    df["phone"] = df["phone"].fillna("000-000-0000")
    if phone_missing > 0:
        logger.info(f"Filled {phone_missing} missing phone numbers with placeholder '000-000-0000'")
    
    # Normalize dates to YYYY-MM-DD format
    df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    
    # FILL: Replace missing dates with placeholders
    dob_missing = df["date_of_birth"].isna().sum()
    df["date_of_birth"] = df["date_of_birth"].fillna(pd.Timestamp("1900-01-01"))
    if dob_missing > 0:
        logger.info(f"Filled {dob_missing} missing date_of_birth with placeholder '1900-01-01'")
    
    created_missing = df["created_date"].isna().sum()
    df["created_date"] = df["created_date"].fillna(pd.Timestamp.now())
    if created_missing > 0:
        logger.info(f"Filled {created_missing} missing created_date with current date")
    
    # FILL: Replace missing address with placeholder
    address_missing = (df["address"].isna() | (df["address"] == "") | (df["address"] == "nan")).sum()
    df["address"] = df["address"].replace(["", "nan"], "Address Not Provided")
    df["address"] = df["address"].fillna("Address Not Provided")
    if address_missing > 0:
        logger.info(f"Filled {address_missing} missing addresses with placeholder")
    
    # FILL: Replace missing income with 0.0
    income_missing = df["income"].isna().sum()
    df["income"] = df["income"].fillna(0.0)
    if income_missing > 0:
        logger.info(f"Filled {income_missing} missing income values with 0.0")
    
    # FILL: Handle account_status missing values
    status_missing = (df["account_status"].isna() | (df["account_status"] == "") | (df["account_status"] == "nan")).sum()
    df["account_status"] = df["account_status"].replace(["nan", ""], "inactive")
    df["account_status"] = df["account_status"].fillna("inactive")
    if status_missing > 0:
        logger.info(f"Filled {status_missing} missing account_status with 'inactive'")
    
    # Re-validate after cleaning 
    cleaned_rows = []
    flagged_rows = []
    
    for index, row in df.iterrows():
        try:
            # Convert row to dict and handle type conversions
            row_dict = row.to_dict()
            
            # Convert customer_id to int
            row_dict["customer_id"] = int(row_dict["customer_id"])
            
            # Convert income to float
            row_dict["income"] = float(row_dict["income"])
            
            # Convert pandas Timestamp to date (YYYY-MM-DD format)
            if isinstance(row_dict["date_of_birth"], pd.Timestamp):
                row_dict["date_of_birth"] = row_dict["date_of_birth"].date()
                
            if isinstance(row_dict["created_date"], pd.Timestamp):
                row_dict["created_date"] = row_dict["created_date"].date()
            
            # Validate with Pydantic
            Customer(**row_dict)
            cleaned_rows.append(row)
            
        except ValidationError as e:
            logger.warning(f"Row {index} validation failed after cleaning: {e}")
            flagged_rows.append({
                "row_index": index,
                "customer_id": row.get("customer_id"),
                "error": str(e)
            })
        except (ValueError, TypeError) as e:
            logger.warning(f"Row {index} type conversion failed: {e}")
            flagged_rows.append({
                "row_index": index,
                "customer_id": row.get("customer_id"),
                "error": str(e)
            })
    
    cleaned_count = len(cleaned_rows)
    flagged_count = len(flagged_rows)
    deleted_count = initial_count - len(df)
    
    logger.info(f"Cleaning summary:")
    logger.info(f"  Initial rows: {initial_count}")
    logger.info(f"  Deleted (missing customer_id): {deleted_count}")
    logger.info(f"  Cleaned successfully: {cleaned_count}")
    logger.info(f"  Failed validation: {flagged_count}")
    
    if cleaned_count == 0:
        logger.error("No valid rows after cleaning")
        raise ValueError("All rows failed validation")
    
    cleaned_df = pd.DataFrame(cleaned_rows)
    cleaned_df = cleaned_df.reset_index(drop=True)
    logger.info("Reset index for cleaned data")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    
    try:
        cleaned_df.to_csv(output_path, index=False, date_format='%Y-%m-%d')
        logger.info(f"Saved {cleaned_count} cleaned rows to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write output file: {e}")
        raise
    
    return {
        "initial_count": initial_count,
        "deleted_count": deleted_count,
        "cleaned_count": cleaned_count,
        "flagged_count": flagged_count,
        "flagged_rows": flagged_rows
    }



