# utils/validation_monitor.py

import json
from pathlib import Path
from datetime import datetime

VALIDATION_LOG = Path(__file__).resolve().parents[1] / "logs" / "validation_status.json"
CRITICAL_FILES = ["UTP_Project_Info.xlsx", "BigData.xlsx"]


def check_validation_status():
    if not VALIDATION_LOG.exists():
        raise FileNotFoundError(f"❌ Validation status log not found at {VALIDATION_LOG}")

    with open(VALIDATION_LOG, "r") as f:
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
        print(f"✅ All files passed validation as of {datetime.now().isoformat(timespec='seconds')}")