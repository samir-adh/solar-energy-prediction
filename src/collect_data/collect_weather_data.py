from WeatherCollector import WeatherCollector
import logging
from datetime import datetime

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    # Configuration
    weather_collector = WeatherCollector(
        save_dir="data/raw/weather",
        logger=logger  # optionnel
    )

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 1, 1)

    # Collection automatique (30 derniers jours)
    weather_collector.save_data(
        start_date=start_date,
        end_date=end_date
    )

    # # Collection période spécifique
    # weather_collector.save_data(
    #     start_date="2022-01-01",
    #     end_date="2024-12-31",
    #     granularity="hourly"  # ou "daily"
    # )

if __name__=='__main__':
    main()