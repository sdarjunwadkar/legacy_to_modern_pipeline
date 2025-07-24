# utils/debounce_mode.py

import os
from datetime import datetime

def is_debounce_mode_active() -> bool:
    """
    Returns True if debounce mode should be active:
    - If current month is between May and August
    - OR if ACTIVE_WATCHER_MODE=true is set in env
    """
    override = os.getenv("ACTIVE_WATCHER_MODE", "false").lower() == "true"
    today = datetime.today()
    in_window = 5 <= today.month <= 8  # May to August
    return override or in_window