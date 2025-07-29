# dags/daily_promotion_dag.py

import os
import sys
import json
from datetime import datetime
from airflow import DAG
import logging
log = logging.getLogger(__name__)
from airflow.operators.python import PythonOperator

# Ensure full project root is added to sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.file_promotion import promote_to_bronze

from pathlib import Path

def promote_validated_files():
    project_root = Path(__file__).resolve().parents[1]  # ← this gives you legacy_to_modern_pipeline/
    validation_log_path = project_root / "logs" / "validation_status.json"

    if not validation_log_path.exists():
        print(f"⚠️ No validation log found at {validation_log_path}")
        return

    with open(validation_log_path, "r") as f:
        validation_status = json.load(f)

    for file_name, info in validation_status.items():
        if info.get("status") == "passed":
            print(f"📦 Promoting {file_name}...")
            log.info(f"📦 Promoting {file_name}...")
            promote_to_bronze(file_name)
        else:
            print(f"⛔ Skipping {file_name} (status: {info.get('status')})")

default_args = {
    "owner": "data_quality_team",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
}

# 12:00 PM CST = 17:00 UTC
with DAG(
    dag_id="daily_bronze_promotion_dag",
    default_args=default_args,
    schedule_interval="0 17 * * 1-5",  # Mon–Fri at 12:00 PM CST
    catchup=False,
    description="Promote validated files to bronze layer daily at noon",
    tags=["data_quality", "promotion"],
) as dag:

    promote_task = PythonOperator(
        task_id="promote_validated_files",
        python_callable=promote_validated_files,
    )