from data.utils.ccil_scraper import CCILScraper
from data.utils.database_service import DatabaseService

def main():
    """
    Main function to fetch data from CCIL, process it, and insert it into the database.
    """
    # Instantiate the CCILScraper and DatabaseService
    scraper = CCILScraper()
    db = DatabaseService()

    # Fetch and process data
    data = scraper.fetch_data()
    df = scraper.process_data(data)

    # Ensure the database table exists
    db.ensure_table_exists()

    # Insert data into the database
    db.insert_data(df)

if __name__ == "__main__":
    main()
