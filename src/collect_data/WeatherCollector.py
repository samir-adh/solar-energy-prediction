from functools import reduce
import os
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from logging import Logger
import logging
from typing import Optional, Dict, Any, List
from tqdm import tqdm

from src.collect_data.WeatherStation import WeatherStation
from timeutils import date_to_int, int_to_date


class WeatherCollector:
    def __init__(self, save_dir: str, stations: list[WeatherStation] | None = None, logger: Logger | None = None) -> None:
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.save_dir = save_dir

        # Stations stratégiques françaises avec coordonnées
        if not stations:
            self.stations = [
                WeatherStation("Paris", 48.8566, 2.3522),
                WeatherStation("Lyon", 45.7640, 4.8357),
                WeatherStation("Marseille", 43.2965, 5.3698),
                WeatherStation("Toulouse", 43.6047, 1.4442),
                WeatherStation("Brest", 48.3904, -4.4861)
            ]
        else:
            self.stations = stations

        # self.stations = {
        #     'paris': {'lat': 48.8566, 'lon': 2.3522, 'name': 'Paris'},
        #     'lyon': {'lat': 45.7640, 'lon': 4.8357, 'name': 'Lyon'},
        #     'marseille': {'lat': 43.2965, 'lon': 5.3698, 'name': 'Marseille'},
        #     'toulouse': {'lat': 43.6047, 'lon': 1.4442, 'name': 'Toulouse'},
        #     'brest': {'lat': 48.3904, 'lon': -4.4861, 'name': 'Brest'}
        # }

        # Variables énergétiques critiques
        self.daily_params = [
            'temperature_2m_mean',
            'temperature_2m_max',
            'temperature_2m_min',
            'precipitation_sum',
            'wind_speed_10m_max',
            'wind_direction_10m_dominant'
        ]

        self.hourly_params = [
            'direct_normal_irradiance',
            'cloud_cover',
            'sunshine_duration',
            'precipitation',
            'surface_pressure',
            'relative_humidity_2m',
            'temperature_2m',
            'wind_speed_10m',
            'shortwave_radiation'
        ]

        if logger is None:
            logging.basicConfig(level=logging.INFO)
            logger = logging.getLogger(__name__)
        self.logger = logger

    def fetch_station_data(self,
                           station:  WeatherStation,
                           start_date: datetime,  # str,
                           end_date: datetime,  # str,
                           granularity: str = 'hourly',
                           ) -> Dict[str, Any]:
        """
        Récupère données météo pour une station donnée

        Args:
            station_key: Clé de la station ('paris', 'lyon', etc.)
            start_date: Format "YYYY-MM-DD"
            end_date: Format "YYYY-MM-DD"  
            granularity: 'hourly' ou 'daily'
        """

        params = {
            'latitude': station.latitude,
            'longitude': station.longitude,
            'start_date': start_date.strftime("%Y-%m-%d"),
            'end_date': end_date.strftime("%Y-%m-%d"),
            'timezone': 'Europe/Paris'  # Fuseau français
        }

        # Sélection des paramètres selon granularité
        if granularity == 'hourly':
            params['hourly'] = ','.join(self.hourly_params)
        else:
            params['daily'] = ','.join(self.daily_params)

        try:
            self.logger.info(
                f"Fetching {granularity} data for {station.name}")
            self.logger.info(f"Period: {start_date} to {end_date}")

            response = requests.get(self.base_url, params=params)

            result = {
                'status_code': response.status_code,
                'url': response.url,
                'station_name': station.name,
                'granularity': granularity
            }

            if response.status_code == 200:
                data = response.json()
                result['success'] = True
                result['data'] = data

                # Compter points de données
                if granularity == 'hourly' and 'hourly' in data:
                    result['data_points'] = len(data['hourly']['time'])
                elif granularity == 'daily' and 'daily' in data:
                    result['data_points'] = len(data['daily']['time'])
                else:
                    result['data_points'] = 0

                self.logger.info(
                    f"✅ {result['data_points']} data points fetched for {station.name}")
            elif response.status_code == 429:
                result['success'] = False
                result['error'] = "Rate limit exceeded"
                self.logger.error(
                    f"❌ Rate limit exceeded for {station.name}: {response.text}")
                time.sleep(60)
                return self.fetch_station_data(
                    station, start_date, end_date, granularity)

            else:
                result['success'] = False
                result['error'] = response.text
                self.logger.error(
                    f"❌ Error {response.status_code} for {station.name}: {response.text}")

            return result

        except Exception as e:
            self.logger.error(f"❌ Exception for {station.name}: {str(e)}")
            return {"error": str(e), "success": False, "station name": station.name}

    def fetch_all_stations_data(self,
                                start_date: datetime,
                                end_date: datetime,
                                granularity: str = 'hourly') -> Dict[str, Any]:
        """
        Récupère données météo pour toutes les stations
        """
        all_results = {}
        for station in self.stations:
            results = self.fetch_station_data(
                station, start_date, end_date, granularity)
            all_results[station.name] = results

        return all_results

    def process_station_data(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Convertit données JSON Open-Meteo en DataFrame formaté
        """
        if not raw_data.get('success') or 'data' not in raw_data:
            return pd.DataFrame()

        data = raw_data['data']
        granularity = raw_data['granularity']
        station_key = raw_data['station_name']

        # Sélection des données selon granularité
        if granularity == 'hourly' and 'hourly' in data:
            time_data = data['hourly']['time']
            weather_data = data['hourly']
        elif granularity == 'daily' and 'daily' in data:
            time_data = data['daily']['time']
            weather_data = data['daily']
        else:
            self.logger.warning(
                f"No {granularity} data found for {station_key}")
            return pd.DataFrame()

        # Construction du DataFrame
        df_data = {'datetime': time_data}

        # Ajout de toutes les variables météo
        for param in weather_data.keys():
            if param != 'time':
                df_data[param] = weather_data[param]

        df = pd.DataFrame(df_data)
        # Conversion datetime et ajout métadonnées
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['station'] = station_key
        df['station_name'] = station_key

        # Conversion timestamp unix (comme votre RTECollector)
        df['timestamp'] = df['datetime'].apply(
            lambda x: date_to_int(x.isoformat()))

        # Réorganisation colonnes
        cols = ['datetime', 'timestamp', 'station', 'station_name'] + \
            [col for col in df.columns if col not in [
                'datetime', 'timestamp', 'station', 'station_name']]
        df = pd.DataFrame(df[cols])

        return df

    def save_data(self,
                  start_date: datetime,  # Optional[str] = None,
                  end_date: datetime,  # Optional[str] = None,
                  granularity: str = 'hourly'):
        """
        Sauvegarde données météo au format CSV (similaire à RTECollector.save_data)
        """
        # Dates par défaut (même logique que RTECollector)
        if start_date is None:
            start_date = datetime.now() - timedelta(days=5)
            # start_date = start_date_datetime.strftime("%Y-%m-%d")

        if end_date is None:
            end_date = datetime.now() - timedelta(days=1)
            # end_date = end_date_datetime.strftime("%Y-%m-%d")

        self.logger.info(f"Starting weather data collection")
        self.logger.info(f"Period: {start_date} to {end_date}")
        self.logger.info(f"Granularity: {granularity}")
        self.logger.info(
            f"Stations: {[stations.name for stations in self.stations]}")

        # Récupération données toutes stations
        all_results = self.fetch_all_stations_data(
            start_date, end_date, granularity)

        # Traitement et sauvegarde par station
        successful_saves = 0

        for station_key, result in all_results.items():
            if result.get('success'):
                df = self.process_station_data(result)

                if not df.empty:
                    # Création répertoire si nécessaire
                    if not os.path.exists(self.save_dir):
                        os.makedirs(self.save_dir)

                    # Nom fichier : STATION_GRANULARITY.csv
                    filename = f"{station_key}_{granularity}.csv"
                    filepath = os.path.join(self.save_dir, filename)

                    # Sauvegarde CSV
                    df.to_csv(filepath, index=False)

                    self.logger.info(
                        f"Saved {station_key}: {len(df)} samples → {filename}")
                    successful_saves += 1
                else:
                    self.logger.warning(
                        f"Empty DataFrame for {station_key}")
            else:
                self.logger.error(
                    f"Failed to fetch data for {station_key}: {result.get('error')}")

        self.logger.info(
            f"Weather collection complete: {successful_saves}/{len(self.stations)} stations saved")

        return successful_saves

    def get_available_stations(self) -> list[WeatherStation]:
        """
        Retourne la liste des stations disponibles
        """
        return self.stations

    def add_custom_station(self, name: str, lat: float, lon: float):
        """
        Ajoute une station personnalisée
        """
        self.stations.append(WeatherStation(name, lat, lon))
        self.logger.info(f"Added custom station: {name}")
