# utils/validation_monitor.py

import json
import os
from pathlib import Path
from datetime import datetime

VALIDATION_LOG = Path(__file__).resolve().parents[1] / "logs" / "validation_status.json"
CRITICAL_FILES = ["UTP_Project_Info.xlsx", "BigData.xlsx"]


def check_validation_status():
    validation_log = Path(os.getenv("VALIDATION_LOG", VALIDATION_LOG))
    if not validation_log.exists():
        raise FileNotFoundError(f"❌ Validation status log not found at {validation_log}")

    with open(validation_log, "r") as f:
        data = json.load(f)

    failed_files = []
    for file in CRITICAL_FILES:
        entry = data.get(file)
        if not entry:
            failed_files.append(f"{file} (missing log)")
            continue

        if entry["status"] != "passed":
            details = entry.get("details", {})
            if "failed_sheets" in details:
                sheet_list = ", ".join(details["failed_sheets"])
                failed_files.append(f"{file} — failed sheets: [{sheet_list}]")
            elif "notes" in details:
                failed_files.append(f"{file} — {details['notes']}")
            else:
                failed_files.append(f"{file} (status: {entry['status']})")

    if failed_files:
        msg = (
            f"\n🚨 Validation issues detected as of {datetime.now().isoformat(timespec='seconds')}:\n"
            + "\n".join(f" - {f}" for f in failed_files)
        )
        print(msg)
        raise ValueError(msg + "\n❌ Validation check failed")
    else:
         # If all critical files passed and promotion is active
        from os import getenv
        if getenv("ALERT_ONLY_MODE", "true").lower() != "true":
            trigger_dag("daily_bronze_promotion_dag")
        print(f"✅ All files passed validation as of {datetime.now().isoformat(timespec='seconds')}")

def trigger_dag(dag_id: str):
    """
    Triggers the given DAG using Airflow's local client.
    This wrapper is added for testing/mocking purposes.
    """
    try:
        from airflow.api.client.local_client import Client
        client = Client()
        client.trigger_dag(dag_id)
        print(f"✅ DAG '{dag_id}' triggered successfully.")
    except Exception as e:
        print(f"❌ Failed to trigger DAG {dag_id}: {e}")
        raise