# unit tests for great expectations validation suites and report formatting
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "integrations"))
from data_quality.checks import format_report_text, validate_dataframe


def test_dim_fx_passes_valid_data():
    df = pd.DataFrame({
        "rate_date": ["2025-06-01", "2025-06-02", "2025-06-03"],
        "usd_per_eur": [1.08, 1.09, 1.10],
    })
    report = validate_dataframe("dim_fx", df)
    assert report["success"] is True
    assert report["passed"] == report["total"]
    assert report["failures"] == []


def test_dim_fx_catches_duplicates_and_out_of_range():
    df = pd.DataFrame({
        "rate_date": ["2025-06-01", "2025-06-01"],
        "usd_per_eur": [None, 2.5],
    })
    report = validate_dataframe("dim_fx", df)
    assert report["success"] is False
    failed_types = {f["expectation"] for f in report["failures"]}
    assert "expect_column_values_to_be_unique" in failed_types
    assert "expect_column_values_to_not_be_null" in failed_types
    assert "expect_column_values_to_be_between" in failed_types


def test_format_report_text_includes_status_and_failures():
    sample = [{
        "table": "dim_fx",
        "row_count": 2,
        "success": False,
        "passed": 1,
        "total": 2,
        "failures": [{"expectation": "expect_column_values_to_be_unique", "column": "rate_date", "detail": "2 unexpected rows (100.00%)"}],
    }]
    text = format_report_text(sample)
    assert "[FAIL] dim_fx (2 rows): 1/2 checks passed" in text
    assert "expect_column_values_to_be_unique" in text
