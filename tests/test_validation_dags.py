from tests.test_utils import write_validation_file, run_check_with_env
import pytest

def test_alert_only_mode_failure(temp_validation_status):
    mock_data = {
        "UTP_Project_Info.xlsx": {
            "status": "failed",
            "last_checked": "2025-07-29T11:48:17",
            "details": {
                "failed_sheets": ["CSJ List", "Comments & Actions"]
            }
        }
    }
    write_validation_file(temp_validation_status, mock_data)

    try:
        run_check_with_env(temp_validation_status, alert_only_mode=True)
    except Exception as e:
        assert "Validation check failed" in str(e)
    else:
        assert False, "Expected failure, but check passed."


def test_alert_only_mode_pass(temp_validation_status):
    mock_data = {
        "UTP_Project_Info.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T11:00:00",
            "details": {}
        },
        "BigData.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T11:00:05",
            "details": {}
        }
    }
    write_validation_file(temp_validation_status, mock_data)

    # Should run without exception
    run_check_with_env(temp_validation_status, alert_only_mode=True)

def test_promotion_mode_pass(temp_validation_status):
    mock_data = {
        "UTP_Project_Info.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T11:00:00",
            "details": {}
        },
        "BigData.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T11:00:05",
            "details": {}
        }
    }
    write_validation_file(temp_validation_status, mock_data)
    run_check_with_env(temp_validation_status, alert_only_mode=False)

def test_promotion_mode_failure(temp_validation_status):
    mock_data = {
        "UTP_Project_Info.xlsx": {
            "status": "failed",
            "last_checked": "2025-07-29T11:48:17",
            "details": {
                "failed_sheets": ["CSJ List"]
            }
        },
        "BigData.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T11:00:05",
            "details": {}
        }
    }
    write_validation_file(temp_validation_status, mock_data)
    try:
        run_check_with_env(temp_validation_status, alert_only_mode=False)
    except Exception as e:
        assert "Validation check failed" in str(e)

def test_logging_on_validation_failure(temp_validation_status, capfd):
    mock_data = {
        "UTP_Project_Info.xlsx": {
            "status": "failed",
            "last_checked": "2025-07-30T11:00:00",
            "details": {"failed_sheets": ["CSJ List"]}
        },
        "BigData.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T11:00:05",
            "details": {}
        }
    }
    write_validation_file(temp_validation_status, mock_data)

    with pytest.raises(ValueError):
        run_check_with_env(temp_validation_status, alert_only_mode=True)

    out, _ = capfd.readouterr()
    assert "🚨 Validation issues detected" in out
    assert "UTP_Project_Info.xlsx — failed sheets: [CSJ List]" in out
    assert "BigData.xlsx" not in out  # Should not appear since it's passed


def test_logging_on_validation_pass(temp_validation_status, capfd):
    mock_data = {
        "UTP_Project_Info.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T11:00:00",
            "details": {}
        },
        "BigData.xlsx": {
            "status": "passed",
            "last_checked": "2025-07-30T11:00:05",
            "details": {}
        }
    }
    write_validation_file(temp_validation_status, mock_data)

    run_check_with_env(temp_validation_status, alert_only_mode=True)

    out, _ = capfd.readouterr()
    assert "Validation issues detected" not in out
    assert "UTP_Project_Info.xlsx" not in out