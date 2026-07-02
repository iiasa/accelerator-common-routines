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

    @patch("service.register_validation_via_ipc", return_value=True)
    def test_full_pipeline_sort_with_list_column(
        self, mock_ipc, pathways_template, requests_mock, tmp_path
    ):
        os.environ.pop("VERIFY_ONLY", None)
        os.environ["ACC_JOB_GATEWAY_SERVER"] = "http://fake-gateway:8000"

        url = (
            f"{os.environ['ACC_JOB_GATEWAY_SERVER']}"
            f"/api/v1/ajob-cli/dataset-template-detail/{TEMPLATE_ID}/"
        )
        requests_mock.get(url, json={"rules": pathways_template})

        csv_path = tmp_path / "test_data.csv"
        with open(str(csv_path), "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["model", "scenario", "region", "variable", "item", "units", "year", "value"])
            writer.writerow(["g4m", "baseline", "de00", "area", "all|fl|forest", "ha", "2000", "5000000.0"])
            writer.writerow(["g4m", "baseline", "de00", "area", "all|fl|forest", "ha", "2001", "5100000.0"])
            writer.writerow(["g4m", "baseline", "at00", "area", "all|fl|forest", "ha", "2000", "3836906.5"])
            writer.writerow(["g4m", "baseline", "at00", "area", "all|fl|forest", "ha", "2001", "3842404.75"])

        svc = CsvRegionalTimeseriesVerificationService(
            filename=str(csv_path),
            dataset_template_id=TEMPLATE_ID,
            job_token="test-token",
            original_filepath="test_data.csv",
        )

        svc()

        assert len(svc.errors) == 0, f"Unexpected errors: {svc.errors}"
        assert csv_path.exists(), "Original file should retain sorted content after replace"

    