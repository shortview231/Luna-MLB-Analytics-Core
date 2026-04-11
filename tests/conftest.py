import json
from pathlib import Path

import pytest


@pytest.fixture
def sample_bundle():
    path = Path("tests/fixtures/sample_boxscore_bundle.json")
    return json.loads(path.read_text(encoding="utf-8"))
