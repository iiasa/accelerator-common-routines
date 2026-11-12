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

    

class TestStrictMatchingAndWhitespace:
    """Values must match the template exactly (case-sensitive); stray whitespace is stripped."""

    def _service(self, pathways_template, requests_mock):
        os.environ["ACC_JOB_GATEWAY_SERVER"] = "http://fake-gateway:8000"
        url = (
            f"{os.environ['ACC_JOB_GATEWAY_SERVER']}"
            f"/api/v1/ajob-cli/dataset-template-detail/{TEMPLATE_ID}/"
        )
        requests_mock.get(url, json={"rules": pathways_template})
        svc = CsvRegionalTimeseriesVerificationService(
            filename="unused.csv",
            dataset_template_id=TEMPLATE_ID,
            job_token="test-token",
        )
        svc.set_csv_regional_validation_rules()
        svc.init_validation_metadata()
        return svc

    def _row(self, **overrides):
        row = {
            "model": "g4m", "scenario": "baseline", "region": "de00",
            "variable": "area", "item": "all|fl|forest", "units": "ha",
            "year": "2000", "value": "5.0",
        }
        row.update(overrides)
        return row

    def test_surrounding_whitespace_is_stripped(self, pathways_template, requests_mock):
        svc = self._service(pathways_template, requests_mock)
        validation_row, _ = svc.validate_row_data(
            self._row(model="g4m ", variable=" area", item="all | fl|forest ")
        )
        assert validation_row["model"] == "g4m"
        assert validation_row["variable"] == "area"
        assert validation_row["item"] == ["all", "fl", "forest"]

    def test_case_mismatch_is_rejected(self, pathways_template, requests_mock):
        svc = self._service(pathways_template, requests_mock)
        with pytest.raises(ValueError, match="DE00"):
            svc.validate_row_data(self._row(region="DE00"))

    def test_header_case_ignored_and_values_preserved(self, pathways_template, requests_mock, tmp_path):
        svc = self._service(pathways_template, requests_mock)
        csv_path = tmp_path / "mixed_header.csv"
        csv_path.write_text(
            "MODEL,Scenario,REGION,Variable,Item,Units,Year,Value\n"
            "g4m,baseline,at00,area,all|fl|forest,Mha,2000,1.5\n",
            encoding="utf-8",
        )
        svc.filename = str(csv_path)
        rows = list(svc.get_validated_rows())

        assert svc.errors == {}
        validation_row, _ = rows[0]
        assert validation_row["region"] == "at00"
        assert validation_row["units"] == "Mha"
