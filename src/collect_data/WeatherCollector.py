from functools import reduce
import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from logging import Logger
import logging
from typing import Optional, Dict, Any, List
from tqdm import tqdm

from timeutils import date_to_int, int_to_date


class WeatherCollector:
    def __init__(self, save_dir: str, logger: Optional[Logger] = None) -> None:
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.save_dir = save_dir

        # Stations stratégiques françaises avec coordonnées
        self.stations = {
            'paris': {'lat': 48.8566, 'lon': 2.3522, 'name': 'Paris'},
            'lyon': {'lat': 45.7640, 'lon': 4.8357, 'name': 'Lyon'},
            'marseille': {'lat': 43.2965, 'lon': 5.3698, 'name': 'Marseille'},
            'toulouse': {'lat': 43.6047, 'lon': 1.4442, 'name': 'Toulouse'},
            'brest': {'lat': 48.3904, 'lon': -4.4861, 'name': 'Brest'}
        }

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
                           station_key: str,
                           start_date: str,
                           end_date: str,
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

        if station_key not in self.stations:
            return {"error": f"Station {station_key} non reconnue", "success": False}

        station = self.stations[station_key]

        params = {
            'latitude': station['lat'],
            'longitude': station['lon'],
            'start_date': start_date,
            'end_date': end_date,
            'timezone': 'Europe/Paris'  # Fuseau français
        }

        # Sélection des paramètres selon granularité
        if granularity == 'hourly':
            params['hourly'] = ','.join(self.hourly_params)
        else:
            params['daily'] = ','.join(self.daily_params)

        try:
            self.logger.info(
                f"Fetching {granularity} data for {station['name']} ({station_key})")
            self.logger.info(f"Period: {start_date} to {end_date}")

            response = requests.get(self.base_url, params=params)

            result = {
                'status_code': response.status_code,
                'url': response.url,
                'station_key': station_key,
                'station_name': station['name'],
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
                    f"✅ {result['data_points']} data points fetched for {station['name']}")

            else:
                result['success'] = False
                result['error'] = response.text
                self.logger.error(
                    f"❌ Error {response.status_code} for {station['name']}: {response.text}")

            return result

        except Exception as e:
            self.logger.error(f"❌ Exception for {station['name']}: {str(e)}")
            return {"error": str(e), "success": False, "station_key": station_key}

    def slice_date_range(self, start_date: str, end_date: str, step_in_hours: int = 24) -> list[tuple[str, str]]:
        start_date_int = date_to_int(start_date)
        end_date_int = date_to_int(end_date)
        timestamp_list: list[str] = []
        step_in_seconds = step_in_hours * 60 * 60
        current_timestamp = start_date_int
        while current_timestamp <= end_date_int:
            timestamp_list.append(int_to_date(current_timestamp))
            current_timestamp += step_in_seconds
        timestamp_list_str = [(timestamp_list[i-1], timestamp_list[i-1])
                              for i in range(len(timestamp_list)-1)]
        return timestamp_list_str

    def fetch_all_stations_data(self,
                                start_date: str,
                                end_date: str,
                                granularity: str = 'hourly') -> Dict[str, Any]:
        """
        Récupère données météo pour toutes les stations
        """
        # dates = self.slice_date_range(start_date, end_date)
        # all_results = {}
        # for station_key in self.stations.keys():
        #     results_list = []
        #     for slice_start, slice_end in dates:
        #         result = self.fetch_station_data(
        #             station_key, slice_start, slice_end, granularity)
        #         results_list.append(result)

        #     station_results = results_list[0]
        #     for result in results_list[1:]:
        #         for key, value in result['data']['hourly'].items():
        #             station_results['data']['hourly'][key] += value
        #     all_results[station_key] = station_results

        #     # Petit délai pour être respectueux de l'API
        #     import time
        #     time.sleep(0.1)
        all_results = {}
        for station_key in self.stations.keys():
            results = self.fetch_station_data(station_key, start_date, end_date, granularity)
            all_results[station_key] = results

        return all_results

    def process_station_data(self, raw_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Convertit données JSON Open-Meteo en DataFrame formaté
        """
        if not raw_data.get('success') or 'data' not in raw_data:
            return pd.DataFrame()

        data = raw_data['data']
        granularity = raw_data['granularity']
        station_key = raw_data['station_key']

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
        df['station_name'] = self.stations[station_key]['name']

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
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  granularity: str = 'hourly'):
        """
        Sauvegarde données météo au format CSV (similaire à RTECollector.save_data)
        """
        # Dates par défaut (même logique que RTECollector)
        if start_date is None:
            start_date_datetime = datetime.now() - timedelta(days=5)
            start_date = start_date_datetime.strftime("%Y-%m-%d")

        if end_date is None:
            end_date_datetime = datetime.now() - timedelta(days=1)
            end_date = end_date_datetime.strftime("%Y-%m-%d")

        self.logger.info(f"Starting weather data collection")
        self.logger.info(f"Period: {start_date} to {end_date}")
        self.logger.info(f"Granularity: {granularity}")
        self.logger.info(f"Stations: {list(self.stations.keys())}")

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

    def get_available_stations(self) -> Dict[str, Dict]:
        """
        Retourne la liste des stations disponibles
        """
        return self.stations

    def add_custom_station(self, key: str, lat: float, lon: float, name: str):
        """
        Ajoute une station personnalisée
        """
        self.stations[key] = {
            'lat': lat,
            'lon': lon,
            'name': name
        }
        self.logger.info(f"Added custom station: {name} ({key})")
