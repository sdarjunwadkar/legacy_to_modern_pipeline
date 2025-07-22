from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import hashlib
import json
import os
import sys

# Ensure project root is in sys.path so dq_checks can be found
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_FILE, "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from dq_checks.gx_validator import run_validation_for_file

WATCH_DIR = "data/incoming"
CACHE_FILE = "logs/file_hash_cache.json"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def compute_md5(file_path):
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

def validate_files():
    print("🔍 Running validation task from Airflow")
    cache = load_cache()
    updated_cache = cache.copy()

    for filename in os.listdir(WATCH_DIR):
        if filename.startswith(".") or not filename.lower().endswith((".xlsx", ".csv")):
            continue

        file_path = os.path.join(WATCH_DIR, filename)
        if not os.path.isfile(file_path):
            continue

        file_hash = compute_md5(file_path)

        if cache.get(filename) != file_hash:
            print(f"[VALIDATING] {filename}")
            run_validation_for_file(filename)
            updated_cache[filename] = file_hash
        else:
            print(f"[SKIP] {filename} unchanged")

    save_cache(updated_cache)

with DAG(
    "file_validation_dag",
    default_args=default_args,
    description="Polls folder and validates new/updated Excel files",
    schedule_interval="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["validation", "great_expectations"],
) as dag:

    validate_task = PythonOperator(
        task_id="validate_files",
        python_callable=validate_files,
    )