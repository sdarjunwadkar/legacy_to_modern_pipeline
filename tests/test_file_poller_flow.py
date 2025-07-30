import tempfile
import shutil
import os
import pytest
from utils.file_hashing import check_and_update_cache

@pytest.fixture
def temp_incoming_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        incoming = os.path.join(tmpdir, "incoming")
        os.makedirs(incoming, exist_ok=True)
        yield incoming

def test_file_poller_detects_change_and_updates_cache(temp_incoming_dir):
    # Simulate one incoming file
    sample_file = os.path.join(temp_incoming_dir, "test.xlsx")
    with open(sample_file, "w") as f:
        f.write("dummy content")

    # Use isolated temp cache file, initialized with valid JSON
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_cache:
        tmp_cache.write("{}")
        tmp_cache.flush()
        cache_path = tmp_cache.name

    try:
        # Step 1: First time - should detect change
        changed = check_and_update_cache(sample_file, cache_file=cache_path)
        assert changed is True

        # Step 2: Second time - no change
        changed = check_and_update_cache(sample_file, cache_file=cache_path)
        assert changed is False

        # Step 3: Modify file - should detect change again
        with open(sample_file, "a") as f:
            f.write("new line")
        changed = check_and_update_cache(sample_file, cache_file=cache_path)
        assert changed is True

    finally:
        os.unlink(cache_path)