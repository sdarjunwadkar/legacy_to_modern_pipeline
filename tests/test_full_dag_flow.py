import os
import json
import tempfile
import shutil
import pytest
from unittest.mock import patch
from utils.file_hashing import compute_md5

@pytest.fixture
def temp_file_env():
    with tempfile.TemporaryDirectory() as tmpdir:
        incoming = os.path.join(tmpdir, "incoming")
        logs = os.path.join(tmpdir, "logs")
        os.makedirs(incoming, exist_ok=True)
        os.makedirs(logs, exist_ok=True)

        sample_file = os.path.join(incoming, "UTP_Project_Info.xlsx")
        with open(sample_file, "w") as f:
            f.write("hello")

        cache_file = os.path.join(logs, "file_hash_cache.json")
        validation_file = os.path.join(logs, "validation_status.json")

        # Write initial validation_status as failed
        with open(validation_file, "w") as vf:
            json.dump({
                "UTP_Project_Info.xlsx": {
                    "status": "failed",
                    "last_checked": "2025-07-28T10:00:00",
                    "details": {"failed_sheets": ["CSJ List"]}
                },
                "BigData.xlsx": {
                    "status": "passed",
                    "last_checked": "2025-07-28T10:00:00",
                    "details": {}
                }
            }, vf, indent=2)

        yield {
            "incoming": incoming,
            "sample_file": sample_file,
            "cache_file": cache_file,
            "validation_file": validation_file
        }

def simulate_validation_pass(file_path, validation_file):
    with open(validation_file, "w") as vf:
        json.dump({
            "UTP_Project_Info.xlsx": {
                "status": "passed",
                "last_checked": "2025-07-30T12:00:00",
                "details": {}
            },
            "BigData.xlsx": {
                "status": "passed",
                "last_checked": "2025-07-30T12:00:00",
                "details": {}
            }
        }, vf, indent=2)

@patch("utils.validation_monitor.trigger_dag")
def test_full_dag_chaining_flow(mock_trigger, temp_file_env):
    os.environ["VALIDATION_LOG"] = temp_file_env["validation_file"]
    os.environ["ALERT_ONLY_MODE"] = "false"

    # Step 1: Simulate a change
    from utils.file_hashing import check_and_update_cache
    changed = check_and_update_cache(temp_file_env["sample_file"], cache_file=temp_file_env["cache_file"])
    assert changed is True

    # Step 2: Simulate validation passing
    simulate_validation_pass(temp_file_env["sample_file"], temp_file_env["validation_file"])

    # Step 3: Run final check
    from utils.validation_monitor import check_validation_status
    check_validation_status()

    # Step 4: Confirm DAG was triggered
    mock_trigger.assert_called_once_with("daily_bronze_promotion_dag")

@patch("utils.validation_monitor.trigger_dag")
def test_full_alert_only_path(mock_trigger, temp_file_env):
    os.environ["VALIDATION_LOG"] = temp_file_env["validation_file"]
    os.environ["ALERT_ONLY_MODE"] = "true"

    # Step 1: Simulate file change
    from utils.file_hashing import check_and_update_cache
    changed = check_and_update_cache(temp_file_env["sample_file"], cache_file=temp_file_env["cache_file"])
    assert changed is True

    # Step 2: Simulate passing validation
    simulate_validation_pass(temp_file_env["sample_file"], temp_file_env["validation_file"])

    # Step 3: Trigger validation check
    from utils.validation_monitor import check_validation_status
    check_validation_status()

    # Step 4: Confirm validation_pm_check_dag triggered
    mock_trigger.assert_not_called()