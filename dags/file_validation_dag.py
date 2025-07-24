# dags/file_validation_dag.py

import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log

# Ensure full project root is added to sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dq_checks.gx_validator import run_validation_for_file
from scripts.file_change_watcher import file_change_watcher
from utils.debounce_mode import is_debounce_mode_active

default_args = {
    "owner": "data_quality_team",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="file_validation_dag",
    default_args=default_args,
    schedule_interval="*/15 * * * *" if is_debounce_mode_active() else "0 6 * * 1",
    catchup=False,
    description="Validate incoming data files using Great Expectations",
    tags=["data_quality", "validation"],
) as dag:
    if is_debounce_mode_active():
        log.info("🚦 Debounce mode is ACTIVE — using file watcher logic")
    else:
        log.info("🟢 Debounce mode is INACTIVE — running full validation tasks")

    if is_debounce_mode_active():
        watcher_task = PythonOperator(
            task_id="watch_and_validate",
            python_callable=file_change_watcher,
        )
    else:
        def validate_utp():
            success = run_validation_for_file("UTP_Project_Info.xlsx")
            if not success:
                raise ValueError("UTP_Project_Info.xlsx validation failed ❌")

        def validate_bigdata():
            success = run_validation_for_file("BigData.xlsx", suite_name="bigdata_suite")
            if not success:
                raise ValueError("BigData.xlsx validation failed ❌")

        validate_utp_task = PythonOperator(
            task_id="validate_utp_project_info",
            python_callable=validate_utp,
        )

        validate_bigdata_task = PythonOperator(
            task_id="validate_bigdata",
            python_callable=validate_bigdata,
        )

        # # Run both validations in parallel
        # [validate_utp_task, validate_bigdata_task]