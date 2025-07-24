# airflow_home/dags/file_validation_dag.py

# airflow_home/dags/file_validation_dag.py

import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Dynamically add project root to sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dq_checks.gx_validator import run_validation_for_file

default_args = {
    "owner": "data_quality_team",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="file_validation_dag",
    default_args=default_args,
    schedule_interval=None,  # Will add cron later if needed
    catchup=False,
    description="Validate incoming data files using Great Expectations",
    tags=["data_quality", "validation"],
) as dag:

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
        python_callable=validate_utp
    )

    validate_bigdata_task = PythonOperator(
        task_id="validate_bigdata",
        python_callable=validate_bigdata
    )

    # Run both validations in parallel
    [validate_utp_task, validate_bigdata_task]