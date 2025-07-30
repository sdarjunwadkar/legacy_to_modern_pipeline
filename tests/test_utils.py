import json
import os
from unittest import mock
from pathlib import Path
import importlib
import types

def write_validation_file(path, content):
    with open(path, "w") as f:
        json.dump(content, f, indent=2)

def run_check_with_env(mock_validation_file, alert_only_mode=True):
    os.environ["ALERT_ONLY_MODE"] = "true" if alert_only_mode else "false"

    # Patch VALIDATION_LOG inside the imported module
    import utils.validation_monitor as monitor
    importlib.reload(monitor)
    monitor.VALIDATION_LOG = Path(mock_validation_file)

    return monitor.check_validation_status()