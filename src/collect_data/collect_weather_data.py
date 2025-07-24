from WeatherCollector import WeatherCollector
import logging
from datetime import datetime
import json
from WeatherStation import WeatherStation


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    with open('locations.json', 'r') as f:
        locations = json.load(f)

    stations = [
        WeatherStation(
            name=location['name'],
            latitude=location['latitude'],
            longitude=location['longitude'])
        for location in locations
    ]

    # Configuration
    weather_collector = WeatherCollector(
        save_dir="data/weather",
        stations=stations,
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


if __name__ == '__main__':
    main()
