import requests
import pandas as pd
import json
from data.utils.validation_model import CCILRecord, ValidationError, retry_on_failure

class CCILScraper:
    """
    A scraper for the CCIL RBI-NDS-OM1 API.
    This class fetches data from the CCIL NDS-OM1 API and processes it into a DataFrame.
    The dataframe is inserted into an Azure SQL database.

    """

    def __init__(self):
        self.api_url = "https://www.ccilindia.com/web/ccil/rbi-nds-om1"
        self.params = {
            "p_p_id": "com_ccil_ndsom_entire_CCILNdsOM_EntirePortlet_INSTANCE_zavb",
            "p_p_lifecycle": "2",
            "p_p_state": "normal",
            "p_p_mode": "view",
            "p_p_resource_id": "ndsom",
        }

    @retry_on_failure
    def fetch_data(self):
        """
        Fetches data from the CCIL NDS-OM1 API.

        Returns:
            dict: Parsed JSON data from the API response.
        """
        response = requests.get(self.api_url, params=self.params)
        response.raise_for_status()
        data = response.json()
        return data
    
    @retry_on_failure
    def process_data(self, data: dict):
        """
        Processes the fetched data into a DataFrame. Validates each record against the CCILRecord model.

        Args:
            data (dict): The raw data fetched from the API.
        Returns:
            pd.DataFrame: A DataFrame containing the processed data.
        """
        if "result1" in data:
            result = json.loads(data["result1"])
            records = []
            for item in result:
                try:
                    record = CCILRecord(**item)
                    records.append(record.dict())
                except ValidationError as e:
                    print(f"Validation error for item {item}: {e}")
            securities_df = pd.DataFrame(result)
            securities_df["download_timestamp"] = pd.Timestamp.now()
            return securities_df
        else:
            raise ValueError("Unexpected data format: 'result1' key not found.")
