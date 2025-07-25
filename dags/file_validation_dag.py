# dags/file_validation_dag.py

import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

# Ensure full project root is added to sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.debounce_mode import is_debounce_mode_active
from utils.dag_utils import build_debounce_tasks, build_full_validation_tasks

default_args = {
    "owner": "data_quality_team",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
}

# Dynamically choose schedule interval
schedule = "*/15 * * * *" if is_debounce_mode_active() else "0 6 * * 1"

with DAG(
    dag_id="file_validation_dag",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,
    description="Validate incoming data files using Great Expectations",
    tags=["data_quality", "validation"],
) as dag:

    if is_debounce_mode_active():
        log.info("🚦 Debounce mode is ACTIVE — using file watcher logic")
        watcher_task = build_debounce_tasks(dag)
    else:
        log.info("🟢 Debounce mode is INACTIVE — running full validation tasks")
        utp_task, bigdata_task = build_full_validation_tasks(dag)