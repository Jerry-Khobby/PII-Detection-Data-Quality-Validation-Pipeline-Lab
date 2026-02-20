import os
import logging
from datetime import datetime
import pandas as pd

from scripts.clean_data import clean_dataset
from scripts.validator import validate_dataset
from scripts.profile_data import profile_data
from scripts.detect_pii import detect_pii
from scripts.mask_pii import mask_dataset
from scripts.logging import setup_logging






# CONFIG
INPUT_FILE = "scripts/data/customers_raw.csv"
CLEANED_FILE = "scripts/data/customers_cleaned.csv"
MASKED_FILE = "scripts/data/customers_masked.csv"

BASE_OUTPUT_DIR = "deliverables/main"

CLEANING_REPORT = f"{BASE_OUTPUT_DIR}/cleaning_log.txt"
VALIDATION_REPORT = f"{BASE_OUTPUT_DIR}/validation_result.txt"
QUALITY_REPORT = f"{BASE_OUTPUT_DIR}/data_quality_report.txt"
PII_REPORT = f"{BASE_OUTPUT_DIR}/pii_detection_report.txt"
MASKING_REPORT = f"{BASE_OUTPUT_DIR}/masking_sample.txt"
PIPELINE_REPORT = f"{BASE_OUTPUT_DIR}/pipeline_execution_report.txt"

os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)



# HELPER: PRETTY FORMATTER
def format_dict(d, indent=0):
    lines = []
    for key, value in d.items():
        if isinstance(value, dict):
            lines.append(" " * indent + f"{key}:")
            lines.extend(format_dict(value, indent + 4))
        else:
            lines.append(" " * indent + f"{key}: {value}")
    return lines


def write_txt(path, lines):
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))



# PIPELINE
def run_pipeline():

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:

        # ===================================================
        # STAGE 1: LOAD
        # ===================================================
        df_raw = pd.read_csv(INPUT_FILE)
        input_rows, input_cols = df_raw.shape

        stage1_lines = [
            "Stage 1: LOAD",
            f"✓ Loaded {os.path.basename(INPUT_FILE)}",
            f"- {input_rows} rows, {input_cols} columns",
            ""
        ]

        # ===================================================
        # STAGE 2: CLEAN
        # ===================================================
        cleaning_result = clean_dataset(INPUT_FILE, CLEANED_FILE)

        stage2_lines = [
            "Stage 2: CLEAN",
            f"- Initial rows: {cleaning_result['initial_count']}",
            f"- Deleted rows (missing customer_id): {cleaning_result['deleted_count']}",
            f"- Successfully cleaned: {cleaning_result['cleaned_count']}",
            f"- Failed after cleaning: {cleaning_result['flagged_count']}",
            ""
        ]

        # ===================================================
        # STAGE 3: VALIDATE
        # ===================================================
        validation_result = validate_dataset(CLEANED_FILE)

        validation_status = "PASS" if validation_result["passed"] else "FAIL"

        stage3_lines = [
            "Stage 3: VALIDATE",
            f"- Total rows checked: {validation_result['total_rows']}",
            f"- Passed rows: {validation_result['passed_count']}",
            f"- Failed rows: {validation_result['failed_count']}",
            f"- Status: {validation_status}",
            ""
        ]

        # Stop pipeline if validation fails
        if not validation_result["passed"]:
            overall_status = "FAILED"
        else:
            overall_status = "SUCCESS"

        # ===================================================
        # STAGE 4: DETECT PII
        # ===================================================
        df_cleaned = pd.read_csv(CLEANED_FILE)
        pii_result = detect_pii(df_cleaned)
        pii_summary = pii_result["summary"]

        stage4_lines = [
            "Stage 4: DETECT PII",
        ]

        total_pii_found = 0
        for key, value in pii_summary.items():
            stage4_lines.append(f"- {key.replace('_', ' ').title()}: {value}")
            total_pii_found += value

        stage4_lines.append("")

        # ===================================================
        # STAGE 5: MASK
        # ===================================================
        mask_result = mask_dataset(CLEANED_FILE, MASKED_FILE)

        stage5_lines = [
            "Stage 5: MASK",
            f"- Rows processed: {mask_result['total_rows']}",
            f"- Rows masked: {mask_result['masked_rows']}",
            ""
        ]

        # ===================================================
        # STAGE 6: SAVE
        # ===================================================
        stage6_lines = [
            "Stage 6: SAVE",
            f"✓ Saved {os.path.basename(MASKED_FILE)}",
            ""
        ]

        # ===================================================
        # FINAL SUMMARY (BUILT FROM REAL RESULTS)
        # ===================================================
        output_rows = mask_result["masked_rows"]

        pii_risk = "MITIGATED" if output_rows == input_rows else "CHECK REQUIRED"

        final_report = [
            "PIPELINE EXECUTION REPORT",
            "=========================",
            f"Timestamp: {timestamp}",
            "",
        ]

        # Append stages sequentially
        final_report.extend(stage1_lines)
        final_report.extend(stage2_lines)
        final_report.extend(stage3_lines)
        final_report.extend(stage4_lines)
        final_report.extend(stage5_lines)
        final_report.extend(stage6_lines)

        final_report.extend([
            "SUMMARY:",
            f"- Input rows: {input_rows}",
            f"- Output rows: {output_rows}",
            f"- Validation: {validation_status}",
            f"- PII Records Found: {total_pii_found}",
            f"- PII Risk: {pii_risk}",
            f"Status: {overall_status} ✓" if overall_status == "SUCCESS" else f"Status: FAILED ✗"
        ])

        write_txt(PIPELINE_REPORT, final_report)

        print("Pipeline completed successfully.")

    except Exception as e:

        logging.exception("Pipeline crashed")

        write_txt(PIPELINE_REPORT, [
            "PIPELINE EXECUTION REPORT",
            "=========================",
            f"Timestamp: {timestamp}",
            "",
            "Status: FAILED ✗",
            f"Error: {str(e)}"
        ])

        print("Pipeline failed. Check logs.")
        
        

if __name__ == "__main__":
    run_pipeline()