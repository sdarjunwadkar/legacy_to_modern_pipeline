# dags/validation_alert_dag.py

import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Ensure full project root is added to sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.validation_monitor import check_validation_status

default_args = {
    "owner": "data_quality_team",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
}

# 10:00 AM CST → 15:00 UTC
with DAG(
    dag_id="validation_alert_dag",
    default_args=default_args,
    schedule="0 15 * * 1-5",  # Every weekday at 15:00 UTC = 10 AM CST
    catchup=False,
    description="Check validation status and raise alert if any file failed",
    tags=["data_quality", "alerts"],
) as dag:

    alert_task = PythonOperator(
        task_id="check_validation_logs",
        python_callable=check_validation_status,
    )

    trigger_promotion = TriggerDagRunOperator(
        task_id="trigger_bronze_promotion_dag",
        trigger_dag_id="daily_bronze_promotion_dag",
        wait_for_completion=False,
        reset_dag_run=True
    )

    alert_task >> trigger_promotion

globals()["dag"] = dag    