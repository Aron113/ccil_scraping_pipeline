import pandas as pd
from unittest.mock import patch, MagicMock
from data.utils.ccil_scraper import CCILScraper
import json

"""
Defines the unit tests for the CCILScraper.
"""

# Sample data for testing
fake_valid_api_response = {
    "result1": json.dumps([
        {
            "ismt_idnt": "TestID1234",
            "ttc": 1000,
            "tta": 1000.5,
            "op": 99.9,
            "hi": 101.0,
            "lo": 98.5,
            "ltp": 100.0,
            "arrow": "down red",
            "indicator": "G",
            "lty": 7.55,
            "prev_trad_rate": 90.33,
            "trade_yeild": 90.34,
            "mrkt_indc": "CONT",
            "book_indc": "RGLR"
        }
    ])
}

fake_invalid_api_response = {
        "result1": json.dumps([
            {"ismt_idnt": "ID123", "ttc": "invalid_int"}
        ])
    }


def test_fetch_data_success():
    """
    Test the fetch_data method of CCILScraper.
    """
    scraper = CCILScraper()
    mock_json = {"result1": "[]"}

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = mock_json
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Simulate a successful API call with valid JSON response
        data = scraper.fetch_data()
        assert data == mock_json
        mock_get.assert_called_once_with(scraper.api_url, params=scraper.params)


def test_process_data_success():
    """
    Test the process_data method of CCILScraper with valid data.
    """
    scraper = CCILScraper()
    data = fake_valid_api_response

    # Simulate a successful API response being processed
    df = scraper.process_data(data)

    assert isinstance(df, pd.DataFrame)
    assert "ismt_idnt" in df.columns
    assert "download_timestamp" in df.columns
    assert df.iloc[0]["ismt_idnt"] == "TestID1234"
    assert df.iloc[0]["arrow"] == "down red"
    assert df.iloc[0]["tta"] == 1000.5
    assert df.iloc[0]["mrkt_indc"] == "CONT"


def test_process_data_missing_result1():
    """
    Test the process_data method of CCILScraper with missing 'result1' key.
    """
    scraper = CCILScraper()

    # Simulate a response without 'result1'
    data = {"wrong_key": []}

    # There should be no value returned
    assert scraper.process_data(data) == None


def test_process_data_invalid_record():
    """
    Test the process_data method of CCILScraper with invalid records.
    """
    scraper = CCILScraper()
    invalid_data = fake_invalid_api_response

    # Simulate an invalid record that gets caught by Try-Except
    df = scraper.process_data(invalid_data)

    # The DataFrame should be empty since the record is invalid
    assert isinstance(df, pd.DataFrame)
    assert "download_timestamp" in df.columns
