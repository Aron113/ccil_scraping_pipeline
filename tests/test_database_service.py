import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from data.utils.database_service import DatabaseService

"""
Defines the unit tests for the DatabaseService.
"""

@pytest.fixture
def mock_connection():
    """
    Fixture to create a mock DB connection and cursor.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


def test_connect(mock_connection):
    """
    Test the connect method of DatabaseService.
    """
    mock_conn, _ = mock_connection

    with patch("pyodbc.connect", return_value=mock_conn) as mock_connect:
        db = DatabaseService()
        conn = db.connect()
        assert conn == mock_conn
        mock_connect.assert_called_once_with(db.connection_string)


def test_ensure_table_exists_creates_table(mock_connection):
    """
    Test the ensure_table_exists method when the table does not exist.
    """
    mock_conn, mock_cursor = mock_connection
    # Simulate table does not exist
    mock_cursor.fetchone.return_value = [0]

    with patch("pyodbc.connect", return_value=mock_conn):
        db = DatabaseService()
        db.ensure_table_exists()

        # Verify SELECT was executed
        mock_cursor.execute.assert_any_call(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
            db.table_name,
        )

        # Verify CREATE TABLE executed
        assert any("CREATE TABLE" in call.args[0] for call in mock_cursor.execute.call_args_list)
        mock_conn.commit.assert_called_once()


def test_ensure_table_exists_table_already_exists(mock_connection):
    """
    Test the ensure_table_exists method when the table already exists.
    """
    mock_conn, mock_cursor = mock_connection

    # Simulate table already exists
    mock_cursor.fetchone.return_value = [1]

    with patch("pyodbc.connect", return_value=mock_conn):
        db = DatabaseService()
        db.ensure_table_exists()

        # CREATE TABLE should not be called
        for call in mock_cursor.execute.call_args_list:
            assert "CREATE TABLE" not in call.args[0]


def test_insert_data(mock_connection):
    """
    Test the insert_data method of DatabaseService.
    """
    mock_conn, mock_cursor = mock_connection
    df = pd.DataFrame([{
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
        "book_indc": "RGLR",
        "download_timestamp": "2025-08-22T03:00:02.637000",
    }])

    with patch("pyodbc.connect", return_value=mock_conn):
        db = DatabaseService()
        db.insert_data(df)

        # Verify INSERT was executed
        assert any("INSERT INTO" in call.args[0] for call in mock_cursor.execute.call_args_list)
        mock_conn.commit.assert_called_once()


def test_fetch_data(mock_connection):
    """
    Test the fetch_data method of DatabaseService.
    """
    mock_conn, _ = mock_connection
    sample_df = pd.DataFrame({"col1": [1, 2]})

    with patch("pyodbc.connect", return_value=mock_conn):
        with patch("pandas.read_sql", return_value=sample_df) as mock_read_sql:
            db = DatabaseService()
            result = db.fetch_data("SELECT * FROM test")
            assert result.equals(sample_df)
            mock_read_sql.assert_called_once()


def test_delete_all_rows(mock_connection):
    """
    Test the delete_all_rows method of DatabaseService.
    """
    mock_conn, mock_cursor = mock_connection

    with patch("pyodbc.connect", return_value=mock_conn):
        db = DatabaseService()
        db.delete_all_rows()

        mock_cursor.execute.assert_called_once_with(f"DELETE FROM {db.table_name}")
        mock_conn.commit.assert_called_once()
