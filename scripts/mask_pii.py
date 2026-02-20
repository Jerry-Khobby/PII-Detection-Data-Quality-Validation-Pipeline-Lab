import pandas as pd
import re
import logging
import os

# Create logs directory


logger = logging.getLogger(__name__)

def mask_name(name):
    """
    Mask name: 'John Doe' → 'J*** D***'
    """
    if pd.isna(name) or not name or name == "Unknown":
        return name
    
    parts = str(name).split()
    masked_parts = []
    
    for part in parts:
        if len(part) > 0:
            masked = part[0] + "***"
            masked_parts.append(masked)
    
    return " ".join(masked_parts)


def mask_email(email):
    """
    Mask email: 'john.doe@gmail.com' → 'j***@gmail.com'
    """
    if pd.isna(email) or not email or email == "noemail@placeholder.com":
        return email
    
    email_str = str(email)
    
    # Split email into local and domain parts
    if "@" in email_str:
        local, domain = email_str.split("@", 1)
        if len(local) > 0:
            masked_local = local[0] + "***"
            return f"{masked_local}@{domain}"
    
    return email_str


def mask_phone(phone):
    """
    Mask phone: '555-123-4567' → '***-***-4567'
    """
    if pd.isna(phone) or not phone or phone == "000-000-0000":
        return phone
    
    phone_str = str(phone)
    
    # Handle XXX-XXX-XXXX format
    if re.match(r'\d{3}-\d{3}-\d{4}', phone_str):
        parts = phone_str.split("-")
        return f"***-***-{parts[2]}"
    
    return phone_str


def mask_address(address):
    """
    Mask address: '123 Main St' → '[MASKED ADDRESS]'
    """
    if pd.isna(address) or not address or address == "Address Not Provided":
        return address
    
    return "[MASKED ADDRESS]"


def mask_dob(dob):
    """
    Mask date of birth: '1985-03-15' → '1985-**-**'
    """
    if pd.isna(dob):
        return dob
    
    dob_str = str(dob)
    
    # Handle YYYY-MM-DD format
    if re.match(r'\d{4}-\d{2}-\d{2}', dob_str):
        year = dob_str[:4]
        return f"{year}-**-**"
    
    return dob_str


def mask_dataset(input_path, output_path):
    """
    Apply PII masking to cleaned dataset
    """
    logger.info(f"Starting PII masking: {input_path}")
    
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
    
    # Apply masking functions
    logger.info("Applying masking to first_name")
    df["first_name"] = df["first_name"].apply(mask_name)
    
    logger.info("Applying masking to last_name")
    df["last_name"] = df["last_name"].apply(mask_name)
    
    logger.info("Applying masking to email")
    df["email"] = df["email"].apply(mask_email)
    
    logger.info("Applying masking to phone")
    df["phone"] = df["phone"].apply(mask_phone)
    
    logger.info("Applying masking to address")
    df["address"] = df["address"].apply(mask_address)
    
    logger.info("Applying masking to date_of_birth")
    df["date_of_birth"] = df["date_of_birth"].apply(mask_dob)
    
    # Create output directory if needed
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df)} masked rows to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write output file: {e}")
        raise
    
    logger.info("PII masking complete")
    
    return {
        "total_rows": initial_count,
        "masked_rows": len(df)
    }


def compare_datasets(original_path, masked_path, num_samples=5):
    """
    Compare original and masked datasets side by side
    """
    print(f"\n{'='*80}")
    print("BEFORE/AFTER COMPARISON")
    print(f"{'='*80}\n")
    
    try:
        df_original = pd.read_csv(original_path)
        df_masked = pd.read_csv(masked_path)
        
        # Select PII columns for comparison
        pii_columns = ["first_name", "last_name", "email", "phone", "address", "date_of_birth"]
        
        print(f"Showing first {num_samples} rows:\n")
        
        for idx in range(min(num_samples, len(df_original))):
            print(f"Row {idx + 1}:")
            print("-" * 80)
            
            for col in pii_columns:
                if col in df_original.columns and col in df_masked.columns:
                    original_val = df_original.iloc[idx][col]
                    masked_val = df_masked.iloc[idx][col]
                    print(f"  {col:20} | Original: {original_val:30} | Masked: {masked_val}")
            
            print()
        
        # Summary statistics
        print(f"\n{'='*80}")
        print("MASKING SUMMARY")
        print(f"{'='*80}")
        print(f"Total rows processed: {len(df_masked)}")
        print(f"\nPII fields masked:")
        for col in pii_columns:
            if col in df_masked.columns:
                print(f"  ✓ {col}")
        print()
        
    except Exception as e:
        logger.error(f"Failed to compare datasets: {e}")
        print(f"Error comparing datasets: {e}")



