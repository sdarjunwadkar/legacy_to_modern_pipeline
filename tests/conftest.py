import pytest
import tempfile
import os
import json

@pytest.fixture
def temp_validation_status():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "validation_status.json")
        yield filepath