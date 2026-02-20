import re
import pandas as pd
import logging
from pathlib import Path

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
PHONE_REGEX = r"^\+?[\d\s().-]{7,20}$"

LOG_FILE = "../logs/pii_detection.log"
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def detect_pii(df):
    results = {}
    summary = {}

    if "email" in df.columns:
        email_mask = df["email"].astype(str).str.match(EMAIL_REGEX, na=False)
        emails_df = df[email_mask].copy()
        results["emails"] = emails_df
        summary["emails_found"] = len(emails_df)

    if "phone" in df.columns:
        phone_mask = df["phone"].astype(str).str.match(PHONE_REGEX, na=False)
        phones_df = df[phone_mask].copy()
        results["phones"] = phones_df
        summary["phones_found"] = len(phones_df)


    if {"first_name", "last_name", "email"}.issubset(df.columns):
        identity_mask = (
            df["first_name"].notna() &
            df["last_name"].notna() &
            df["email"].astype(str).str.match(EMAIL_REGEX, na=False)
        )
        identity_df = df[identity_mask].copy()
        results["full_identity"] = identity_df
        summary["full_identity_found"] = len(identity_df)


    if "address" in df.columns:
        address_df = df[df["address"].notna()].copy()
        results["addresses"] = address_df
        summary["addresses_found"] = len(address_df)


    if "date_of_birth" in df.columns:
        dob_df = df[df["date_of_birth"].notna()].copy()
        results["dob"] = dob_df
        summary["dob_found"] = len(dob_df)

    return {
       "details":results,
       "summary":summary,
   }





