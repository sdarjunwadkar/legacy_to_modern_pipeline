# utils/file_promotion.py

import shutil
from pathlib import Path
from datetime import datetime

INCOMING_DIR = Path("data/incoming")
BRONZE_DIR = Path("data/bronze")

def promote_to_bronze(file_name: str):
    src = INCOMING_DIR / file_name
    dst = BRONZE_DIR / file_name

    if not src.exists():
        print(f"❌ Source file does not exist: {src}")
        return False

    # Copy (or move, if preferred)
    shutil.copy2(src, dst)
    print(f"[{datetime.now()}] ✅ Promoted {file_name} to bronze/")
    return True