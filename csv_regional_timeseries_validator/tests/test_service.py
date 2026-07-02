import os
import csv
import json
import pytest
from unittest.mock import patch
from service import CsvRegionalTimeseriesVerificationService


TEMPLATE_ID = 999


class TestPathwaysRealData:
    """Integration-style tests with the real Pathways CSV + template, network mocked."""

    def test_validates_all_rows_successfully(
        self, pathways_template, pathways_csv_path, requests_mock
    ):
        os.environ["VERIFY_ONLY"] = "True"
        os.environ["ACC_JOB_GATEWAY_SERVER"] = "http://fake-gateway:8000"

        url = (
            f"{os.environ['ACC_JOB_GATEWAY_SERVER']}"
            f"/api/v1/ajob-cli/dataset-template-detail/{TEMPLATE_ID}/"
        )
        requests_mock.get(url, json={"rules": pathways_template})

        svc = CsvRegionalTimeseriesVerificationService(
            filename=pathways_csv_path,
            dataset_template_id=TEMPLATE_ID,
            job_token="test-token",
            original_filepath="Pathways_8p1_G4M.csv",
        )

        svc()

        assert len(svc.errors) == 0, f"Unexpected errors: {svc.errors}"

    