import os
import json
import tempfile
from unittest.mock import patch
from utils.validation_monitor import check_validation_status

def test_dag_chaining_on_promotion_mode():
    # Prepare fake validation_status.json with passing files
    mock_data = {
        "UTP_Project_Info.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T12:00:00",
            "details": {}
        },
        "BigData.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T12:00:05",
            "details": {}
        }
    }

    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        json.dump(mock_data, tmp)
        tmp.flush()

        # Patch ENV to set ALERT_ONLY_MODE=false
        os.environ["ALERT_ONLY_MODE"] = "false"
        os.environ["VALIDATION_LOG"] = tmp.name

        with patch("utils.validation_monitor.trigger_dag") as mock_trigger:
            check_validation_status()
            mock_trigger.assert_called_once_with("daily_bronze_promotion_dag")