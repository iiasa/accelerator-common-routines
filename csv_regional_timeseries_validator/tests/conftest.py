import os
import json
import pytest


@pytest.fixture
def pathways_template():
    tpl_path = os.path.join(
        os.path.dirname(__file__),
        "templates",
        "pathways.json",
    )
    with open(tpl_path) as f:
        return json.load(f)


@pytest.fixture
def pathways_csv_path():
    path = os.path.expanduser("~/Downloads/Pathways_8p1_G4M.csv")
    if not os.path.exists(path):
        pytest.skip(f"CSV file not found at {path}")
    return path
