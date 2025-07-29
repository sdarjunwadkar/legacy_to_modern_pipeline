# scripts/file_poller.py

import os
import sys
import time
import hashlib
from dq_checks.gx_validator import run_validation_for_file
import logging
from utils.file_hashing import compute_md5, load_cache, save_cache
from pathlib import Path

# Add project root to sys.path
current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
sys.path.insert(0, str(project_root))

log_file = "logs/file_poller.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.getLogger().addHandler(logging.StreamHandler())


POLL_INTERVAL = 1800  # seconds (30 mins)
WATCH_DIR = "data/incoming"

def main():
    logging.info(f"🔁 Starting file poller (every {POLL_INTERVAL} seconds)")
    logging.info(f"📁 Watching folder: {WATCH_DIR}")

    hash_cache = load_cache()

    while True:
        updated = False
        for filename in os.listdir(WATCH_DIR):
            if filename.startswith(".") or not filename.lower().endswith((".csv", ".xlsx")):
                continue

            file_path = os.path.join(WATCH_DIR, filename)
            if not os.path.isfile(file_path):
                continue

            try:
                file_hash = compute_md5(file_path)

                if filename not in hash_cache:
                    logging.info(f"[NEW] {filename}")
                    hash_cache[filename] = file_hash
                    run_validation_for_file(filename)
                    logging.info(f"✅ Validated {filename} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    updated = True

                elif hash_cache[filename] != file_hash:
                    logging.info(f"[UPDATED] {filename}")
                    hash_cache[filename] = file_hash
                    run_validation_for_file(filename)
                    logging.info(f"✅ Validated {filename} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    updated = True

                else:
                    logging.info(f"[UNCHANGED] {filename}")

            except Exception as e:
                logging.error(f"❌ Error processing {filename}: {e}")

        if updated:
            save_cache(hash_cache)

        time.sleep(POLL_INTERVAL)

def run_once():
    logging.info("🔁 Running single-pass file poller (Airflow mode)")
    hash_cache = load_cache()
    updated = False

    for filename in os.listdir(WATCH_DIR):
        if filename.startswith(".") or not filename.lower().endswith((".csv", ".xlsx")):
            continue

        file_path = os.path.join(WATCH_DIR, filename)
        if not os.path.isfile(file_path):
            continue

        try:
            file_hash = compute_md5(file_path)

            if filename not in hash_cache:
                logging.info(f"[NEW] {filename}")
                hash_cache[filename] = file_hash
                run_validation_for_file(filename)
                logging.info(f"✅ Validated {filename} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                updated = True

            elif hash_cache[filename] != file_hash:
                logging.info(f"[UPDATED] {filename}")
                hash_cache[filename] = file_hash
                run_validation_for_file(filename)
                logging.info(f"✅ Validated {filename} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                updated = True

            else:
                logging.info(f"[UNCHANGED] {filename}")

        except Exception as e:
            logging.error(f"❌ Error processing {filename}: {e}")

    if updated:
        save_cache(hash_cache)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("\n👋 File poller stopped by user. Exiting gracefully.")