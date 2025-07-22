# scripts/file_poller.py

import os
import time
import hashlib
from dq_checks.gx_validator import run_validation_for_file
import logging

log_file = "logs/file_poller.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.getLogger().addHandler(logging.StreamHandler())


POLL_INTERVAL = 60  # seconds
WATCH_DIR = "data/incoming"

def compute_md5(file_path):
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def main():
    logging.info(f"🔁 Starting file poller (every {POLL_INTERVAL} seconds)")
    logging.info(f"📁 Watching folder: {WATCH_DIR}")
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
                    logging.info(f"[NEW] {filename}")
                    seen_hashes[filename] = file_hash
                    run_validation_for_file(filename)

                elif seen_hashes[filename] != file_hash:
                    logging.info(f"[UPDATED] {filename}")
                    seen_hashes[filename] = file_hash
                    run_validation_for_file(filename)

                else:
                    logging.info(f"[UNCHANGED] {filename}")
            except Exception as e:
                logging.error(f"❌ Error processing {filename}: {e}")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("\n👋 File poller stopped by user. Exiting gracefully.")