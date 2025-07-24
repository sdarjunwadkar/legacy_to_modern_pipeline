# scripts/file_change_watcher.py

import os
import sys

# Ensure project root is in sys.path so dq_checks can be found
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dq_checks.gx_validator import run_validation_for_file
from utils.file_hashing import compute_md5, load_cache, save_cache

WATCH_DIR = "data/incoming"

def file_change_watcher():
    print("🔁 Running Debounce Watcher...")

    cache = load_cache()
    updated_cache = cache.copy()

    for filename in os.listdir(WATCH_DIR):
        if filename.startswith(".") or not filename.lower().endswith((".csv", ".xlsx")):
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
            print(f"[SKIPPED] {filename} unchanged")

    save_cache(updated_cache)
    print("✅ Debounce check completed.")

if __name__ == "__main__":
    file_change_watcher()