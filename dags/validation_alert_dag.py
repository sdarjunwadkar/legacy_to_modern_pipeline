import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pathlib import Path
from dotenv import load_dotenv

# ─── Load .env and project path ───────────────────────────────────────────────
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
dotenv_path = os.path.join(project_root, ".env")
load_dotenv(dotenv_path)

# ─── Flag to control whether promotion DAG should be triggered ────────────────
ALERT_ONLY_MODE = os.getenv("ALERT_ONLY_MODE", "True").lower() == "true"
print(f"[DEBUG] ALERT_ONLY_MODE is {'ENABLED' if ALERT_ONLY_MODE else 'DISABLED'}")

from utils.validation_monitor import check_validation_status

default_args = {
    "owner": "data_quality_team",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="validation_alert_dag",
    default_args=default_args,
    schedule="0 15 * * 1-5",  # 10:00 AM CST = 15:00 UTC
    catchup=False,
    description="Check validation status and raise alert if any file failed",
    tags=["data_quality", "alerts"],
) as dag:

    alert_task = PythonOperator(
        task_id="check_validation_logs",
        python_callable=check_validation_status,
    )

    # Conditionally add promotion trigger
    if not ALERT_ONLY_MODE:
        trigger_promotion = TriggerDagRunOperator(
            task_id="trigger_bronze_promotion_dag",
            trigger_dag_id="daily_bronze_promotion_dag",
            wait_for_completion=False,
            reset_dag_run=True,
        )
        alert_task >> trigger_promotion
    else:
        # Helpful log message
        from airflow.operators.python import PythonOperator

        def log_alert_mode():
            print("🔔 ALERT_ONLY_MODE is True — skipping promotion trigger.")

        log_alert = PythonOperator(
            task_id="log_alert_only_mode",
            python_callable=log_alert_mode,
        )
        alert_task >> log_alert

globals()["dag"] = dag