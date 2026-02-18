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

    if "email" in df.columns:
        email_mask = df["email"].astype(str).str.match(EMAIL_REGEX, na=False)
        results["emails"] = df[email_mask].copy()

    if "phone" in df.columns:
        phone_mask = df["phone"].astype(str).str.match(PHONE_REGEX, na=False)
        results["phones"] = df[phone_mask].copy()

    if {"first_name", "last_name", "email"}.issubset(df.columns):
        identity_mask = (
            df["first_name"].notna() &
            df["last_name"].notna() &
            df["email"].astype(str).str.match(EMAIL_REGEX, na=False)
        )
        results["full_identity"] = df[identity_mask].copy()

    if "address" in df.columns:
        results["addresses"] = df[df["address"].notna()].copy()

    if "date_of_birth" in df.columns:
        results["dob"] = df[df["date_of_birth"].notna()].copy()

    return results




df = pd.read_csv("../scripts/customers_raw.csv")
df.columns = df.columns.str.strip()
print(detect_pii(df))
