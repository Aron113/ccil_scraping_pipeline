import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv
from data.utils.validation_model import retry_on_failure

load_dotenv()

class DatabaseService:
    def __init__(self):
        self.connection_string = os.getenv("AZURE_DB_CONNECTION")
        self.table_name = os.getenv("TABLE_NAME")
        self.conn = None

    @retry_on_failure
    def connect(self):
        """
        Establishes a connection to the Azure SQL database.

        Returns:
            pyodbc.Connection: A connection object to the database.
        """
        if self.conn is None:
            self.conn = pyodbc.connect(self.connection_string)
        return self.conn

    @retry_on_failure
    def ensure_table_exists(self):
        """
        Ensures that the database table exists. If it does not, it creates the table.
        """
        conn = self.connect()
        cursor = conn.cursor()
        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?
        """, self.table_name)
        exists = cursor.fetchone()[0]
        if not exists:
            cursor.execute(f"""
                CREATE TABLE {self.table_name} (
                    ismt_idnt NVARCHAR(32),
                    ttc INT,
                    tta FLOAT,
                    op FLOAT,
                    hi FLOAT,
                    lo FLOAT,
                    ltp FLOAT,
                    arrow NVARCHAR(16),
                    indicator NVARCHAR(4),
                    lty FLOAT,
                    prev_trad_rate FLOAT,
                    trade_yeild FLOAT,
                    mrkt_indc NVARCHAR(8),
                    book_indc NVARCHAR(8),
                    download_timestamp DATETIME
                )
            """)
            conn.commit()
        cursor.close()

    @retry_on_failure
    def insert_data(self, df: pd.DataFrame):
        """
        Inserts data into the database table.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to insert.
        """
        conn = self.connect()
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {self.table_name} (
                    ismt_idnt, ttc, tta, op, hi, lo, ltp, arrow, indicator, lty, prev_trad_rate, trade_yeild, mrkt_indc, book_indc, download_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row['ismt_idnt'], row['ttc'], row['tta'], row['op'], row['hi'], row['lo'], row['ltp'],
            row['arrow'], row['indicator'], row['lty'], row['prev_trad_rate'], row['trade_yeild'],
            row['mrkt_indc'], row['book_indc'], row['download_timestamp'])
        conn.commit()
        cursor.close()

    @retry_on_failure
    def fetch_data(self, query: str):
        """
        Fetches data from the database using the provided SQL query.

        Args:
            query (str): The SQL query to execute.
        Returns:
            pd.DataFrame: A DataFrame containing the results of the query.
        """
        conn = self.connect()
        return pd.read_sql(query, conn)

    @retry_on_failure
    def delete_all_rows(self):
        """
        Deletes all rows from the table.
        """
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {self.table_name}")
        conn.commit()
        cursor.close()
