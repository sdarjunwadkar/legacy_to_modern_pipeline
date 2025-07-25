# scripts/trigger_file_validation_dag.py

import subprocess
# scripts/trigger_file_validation_dag.py

import os
import sys
from datetime import datetime

# Fix: Add actual root directory to sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", ".."))  # <- one more level up
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.file_hashing import compute_md5, load_cache, save_cache
# from scripts.file_change_watcher import FILES_TO_WATCH
from pathlib import Path
from datetime import datetime

WATCH_DIR = "data/incoming"
FILES_TO_WATCH = ["UTP_Project_Info.xlsx", "BigData.xlsx"]

def should_trigger_dag():
    cache = load_cache()
    changed_files = []

    for file_name in FILES_TO_WATCH:
        file_path = Path(WATCH_DIR) / file_name
        if not file_path.exists():
            continue

        new_hash = compute_md5(str(file_path))
        old_hash = cache.get(file_name)

        if new_hash != old_hash:
            changed_files.append(file_name)
            cache[file_name] = new_hash

    if changed_files:
        save_cache(cache)
        print(f"[{datetime.now()}] Change detected in: {changed_files}")
        return True

    print(f"[{datetime.now()}] No file changes detected.")
    return False

def trigger_airflow_dag(dag_id="file_validation_dag"):
    print(f"🔁 Triggering DAG: {dag_id}")
    result = subprocess.run(
        ["airflow", "dags", "trigger", dag_id],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print("❌ Failed to trigger DAG:")
        print(result.stderr)

if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))  # Ensure relative paths work

    if should_trigger_dag():
        trigger_airflow_dag()
    else:
        print("🚫 DAG not triggered.")