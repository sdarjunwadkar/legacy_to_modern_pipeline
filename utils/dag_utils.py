# utils/dag_utils.py

from airflow.operators.python import PythonOperator
from dq_checks.gx_validator import run_validation_for_file
from scripts.file_change_watcher import file_change_watcher

def build_debounce_tasks(dag):
    return PythonOperator(
        task_id="watch_and_validate",
        python_callable=file_change_watcher,
        dag=dag,
    )

def build_full_validation_tasks(dag):
    def validate_utp():
        success = run_validation_for_file("UTP_Project_Info.xlsx")
        if not success:
            raise ValueError("UTP_Project_Info.xlsx validation failed ❌")

    def validate_bigdata():
        success = run_validation_for_file("BigData.xlsx", suite_name="bigdata_suite")
        if not success:
            raise ValueError("BigData.xlsx validation failed ❌")

    utp_task = PythonOperator(
        task_id="validate_utp_project_info",
        python_callable=validate_utp,
        dag=dag,
    )

    bigdata_task = PythonOperator(
        task_id="validate_bigdata",
        python_callable=validate_bigdata,
        dag=dag,
    )

    return [utp_task, bigdata_task]