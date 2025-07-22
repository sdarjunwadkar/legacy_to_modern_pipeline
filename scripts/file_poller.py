# scripts/file_poller.py

import os
import time
import hashlib
from dq_checks.gx_validator import run_validation_for_file

POLL_INTERVAL = 60  # seconds
WATCH_DIR = "data/incoming"

def compute_md5(file_path):
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def main():
    print(f"🔁 Starting file poller (every {POLL_INTERVAL} seconds)")
    print(f"📁 Watching folder: {WATCH_DIR}")
    seen_hashes = {}

    while True:
        for filename in os.listdir(WATCH_DIR):
            # 🚫 Skip hidden files or unsupported types
            if filename.startswith(".") or not filename.lower().endswith((".csv", ".xlsx")):
                continue

            file_path = os.path.join(WATCH_DIR, filename)

            if not os.path.isfile(file_path):
                continue

            try:
                file_hash = compute_md5(file_path)

                if filename not in seen_hashes:
                    print(f"[NEW] {filename}")
                    seen_hashes[filename] = file_hash
                    run_validation_for_file(filename)

                elif seen_hashes[filename] != file_hash:
                    print(f"[UPDATED] {filename}")
                    seen_hashes[filename] = file_hash
                    run_validation_for_file(filename)

                else:
                    print(f"[UNCHANGED] {filename}")
            except Exception as e:
                print(f"❌ Error processing {filename}: {e}")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 File poller stopped by user. Exiting gracefully.")