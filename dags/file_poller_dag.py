import os
import sys
from pathlib import Path

# Ensure full project root is added to sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.file_poller import run_once

default_args = {
    "owner": "data_quality_team",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="file_poller_dag",
    default_args=default_args,
    schedule_interval="*/30 * * * *",  # Every 30 minutes
    catchup=False,
    description="Poll for changed files and trigger validation if needed",
    tags=["data_quality", "polling"],
) as dag:

    poll_task = PythonOperator(
        task_id="run_file_poller_once",
        python_callable=run_once,
    )

globals()["dag"] = dag