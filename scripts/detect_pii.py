import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
PHONE_REGEX = r"^\+?[\d\s().-]{7,20}$"


def detect_pii(df):
    logger.info("Starting PII detection")
    
    results = {}
    summary = {}

    if "email" in df.columns:
        email_mask = df["email"].astype(str).str.match(EMAIL_REGEX, na=False)
        emails_df = df[email_mask].copy()
        results["emails"] = emails_df
        summary["emails_found"] = len(emails_df)
        logger.info(f"Found {len(emails_df)} email addresses")

    if "phone" in df.columns:
        phone_mask = df["phone"].astype(str).str.match(PHONE_REGEX, na=False)
        phones_df = df[phone_mask].copy()
        results["phones"] = phones_df
        summary["phones_found"] = len(phones_df)
        logger.info(f"Found {len(phones_df)} phone numbers")

    if {"first_name", "last_name", "email"}.issubset(df.columns):
        identity_mask = (
            df["first_name"].notna() &
            df["last_name"].notna() &
            df["email"].astype(str).str.match(EMAIL_REGEX, na=False)
        )
        identity_df = df[identity_mask].copy()
        results["full_identity"] = identity_df
        summary["full_identity_found"] = len(identity_df)
        logger.info(f"Found {len(identity_df)} full identity records")

    if "address" in df.columns:
        address_df = df[df["address"].notna()].copy()
        results["addresses"] = address_df
        summary["addresses_found"] = len(address_df)
        logger.info(f"Found {len(address_df)} addresses")

    if "date_of_birth" in df.columns:
        dob_df = df[df["date_of_birth"].notna()].copy()
        results["dob"] = dob_df
        summary["dob_found"] = len(dob_df)
        logger.info(f"Found {len(dob_df)} dates of birth")

    logger.info("PII detection complete")

    return {
       "details": results,
       "summary": summary,
   }
