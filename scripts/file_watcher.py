import time
import os
import hashlib
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

WATCH_FOLDER = "data/incoming"
HASH_CACHE_FILE = "logs/file_hash_cache.json"

# Load or create hash cache
if os.path.exists(HASH_CACHE_FILE):
    with open(HASH_CACHE_FILE, "r") as f:
        file_hashes = json.load(f)
else:
    file_hashes = {}

def compute_file_hash(path):
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

class FileChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return
        filepath = event.src_path
        filename = os.path.basename(filepath)

        if not filename.endswith((".csv", ".xlsx")):
            return

        new_hash = compute_file_hash(filepath)
        old_hash = file_hashes.get(filename)

        if new_hash != old_hash:
            print(f"[UPDATED] {filename}")
            file_hashes[filename] = new_hash

            # Save updated hash cache
            with open(HASH_CACHE_FILE, "w") as f:
                json.dump(file_hashes, f, indent=2)

        else:
            print(f"[UNCHANGED] {filename}")

if __name__ == "__main__":
    print(f"📁 Watching folder: {WATCH_FOLDER}")
    event_handler = FileChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_FOLDER, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()