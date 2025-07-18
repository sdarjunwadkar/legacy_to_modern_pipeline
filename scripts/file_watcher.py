# scripts/file_watcher.py

import time
import os
import hashlib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import sys
from pathlib import Path

# Add the root directory of the project to Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from dq_checks.gx_validator import run_validation_for_file

WATCH_FOLDER = "data/incoming"
hash_cache = {}

def compute_hash(file_path):
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

class FileChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if (
            not event.is_directory
            and event.src_path.endswith((".csv", ".xlsx"))
            and not os.path.basename(event.src_path).startswith("~$")
        ):
            filename = os.path.basename(event.src_path)
            print(f"[DEBUG] Detected: {filename}")
            file_path = os.path.join(WATCH_FOLDER, filename)
            new_hash = compute_hash(file_path)

            if filename not in hash_cache:
                hash_cache[filename] = new_hash
                print(f"[NEW] {filename}")
                run_validation_for_file(filename)
            elif hash_cache[filename] != new_hash:
                hash_cache[filename] = new_hash
                print(f"[UPDATED] {filename}")
                run_validation_for_file(filename)
            else:
                print(f"[UNCHANGED] {filename}")

    def on_created(self, event):
        if (
            not event.is_directory
            and event.src_path.endswith((".csv", ".xlsx"))
            and not os.path.basename(event.src_path).startswith("~$")
        ):
            filename = os.path.basename(event.src_path)
            print(f"[DEBUG] Created: {filename}")
            file_path = os.path.join(WATCH_FOLDER, filename)
            new_hash = compute_hash(file_path)
            hash_cache[filename] = new_hash
            print(f"[NEW] {filename}")
            run_validation_for_file(filename)

if __name__ == "__main__":
    print(f"📁 Watching folder: {WATCH_FOLDER}")
    event_handler = FileChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_FOLDER, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()