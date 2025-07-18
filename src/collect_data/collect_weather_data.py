from WeatherCollector import WeatherCollector
import logging


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    # Configuration
    weather_collector = WeatherCollector(
        save_dir="data/raw/weather",
        logger=logger  # optionnel
    )

    # Collection automatique (30 derniers jours)
    weather_collector.save_data(
        start_date="2020-01-01",
        end_date="2025-01-01"
    )

    # # Collection période spécifique
    # weather_collector.save_data(
    #     start_date="2022-01-01",
    #     end_date="2024-12-31",
    #     granularity="hourly"  # ou "daily"
    # )

if __name__=='__main__':
    main()