from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import os
from timeutils import format_datetime
from RTECollector import RTECollector


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    load_dotenv()
    CLIENT_ID = os.getenv('RTE_CLIENT_ID')
    CLIENT_SECRET = os.getenv('RTE_CLIENT_SECRET')
    if not CLIENT_ID or not CLIENT_SECRET:
        logger.error(
            "Please configure your RTE_CLIENT_ID and RTE_CLIENT_SECRET")
        return

    rte_data_collector = RTECollector(
        CLIENT_ID, CLIENT_SECRET, save_dir="data/energy", production_type="SOLAR", logger=logger)

    start_date = datetime(2020,1,1)
    end_date = datetime(2025,1,1)
    print(f"Fetching data from {start_date} to {end_date}")

    rte_data_collector.save_data(start_date, end_date)
    # result = rte_data_collector.fetch_data(
    #     start_date=start_date, end_date=end_date)


if __name__ == '__main__':
    main()
