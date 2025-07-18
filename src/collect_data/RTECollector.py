import base64
from datetime import datetime, timedelta
from logging import Logger
import logging
import os
from typing import Optional, Dict, Any, AbstractSet

import pandas as pd
import requests

from timeutils import date_to_int
from tqdm import tqdm


class RTECollector():
    def __init__(self, client_id: str, client_secret: str, save_dir: str, logger: Optional[Logger] = None) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://digital.iservices.rte-france.com"
        self.token_url = f"{self.base_url}/token/oauth/"
        self.api_base = f"{self.base_url}/open_api/actual_generation/v1"
        self.access_token = None
        self.token_expires_at = None

        self.save_dir = save_dir

        if logger is None:
            logging.basicConfig(level=logging.INFO)
            logger = logging.getLogger(__name__)
        self.logger = logger

    def get_oauth2_token(self) -> bool:
        """
        Acquire a token, valid for 2 hours
        """
        try:
            # Encode credentials in base64
            credentials = f"{self.client_id}:{self.client_secret}"
            encoded_credentials = base64.b64encode(
                credentials.encode()).decode()

            headers = {
                'Authorization': f'Basic {encoded_credentials}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            # OAuth2 Client Credentials Grant
            data = {'grant_type': 'client_credentials'}

            self.logger.info("Requesting OAuth2 token from RTE...")
            response = requests.post(
                self.token_url, headers=headers, data=data)

            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                # Token valid for 2 hours
                expires_in = token_data.get('expires_in', 7200)
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

                self.logger.info("Successfuly fetched OAuth2 token")
                self.logger.info(f"Token expire at: {self.token_expires_at}")
                return True
            else:
                self.logger.error(
                    f"Error when fetching token: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                return False

        except Exception as e:
            self.logger.error(f"Exception during authentication: {str(e)}")
            return False

    def is_token_valid(self) -> bool:
        """Check if the token is still valid"""
        if not self.access_token or not self.token_expires_at:
            return False
        return datetime.now() < self.token_expires_at

    def ensure_valid_token(self) -> bool:
        """Ensure that a token is available"""
        if not self.is_token_valid():
            return self.get_oauth2_token()
        return True

    def fetch_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        if not self.ensure_valid_token():
            return {"error": "Can't get valid token"}

        # Set default start and end dates
        if not start_date:
            # Default start date : 30 days ago
            start_date = (datetime.now()-timedelta(days=30)
                          ).strftime("%Y-%m-%dT00:00:00+01:00")
        if not end_date:
            # Default end date : yesterday
            end_date = (datetime.now()-timedelta(days=1)
                        ).strftime("%Y-%m-%dT23:59:59+01:00")

        url = f"{self.api_base}/actual_generations_per_production_type"

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }

        params = {
            'start_date': start_date,
            'end_date': end_date
        }

        try:
            self.logger.info(
                f"Fetching data from API: actual_generations_per_production_type")
            self.logger.info(f"Period: from {start_date} to {end_date}")

            response = requests.get(url, headers=headers, params=params)

            result = {
                'status_code': response.status_code,
                'url': response.url,
                'headers': dict(response.headers),
                'params': params
            }

            if response.status_code == 200:
                data = response.json()
                result['success'] = True
                result['data'] = data
                result['data_points'] = len(
                    data.get('actual_generations_per_production_type', []))

                self.logger.info(
                    f"{result['data_points']} data points fetched")

                # Print all available production types
                if 'actual_generations_per_production_type' in data:
                    production_types = set()
                    for item in data['actual_generations_per_production_type']:
                        production_types.add(item.get('production_type'))
                    self.logger.info(
                        f"Production types: {sorted(production_types)}")

            else:
                result['success'] = False
                result['error'] = response.text
                self.logger.error(
                    f"Error {response.status_code}: {response.text}")

            return result
        except Exception as e:
            self.logger.error(f"Exception when fetching data: {str(e)}")
            return {"error": str(e), "success": False}

    def slice_dates(self, start_date: str, end_date: str, step_in_hours: int) -> list[tuple[str, str]]:
        start_date_float = datetime.fromisoformat(start_date).timestamp()
        end_date_float = datetime.fromisoformat(end_date).timestamp()
        step_in_seconds = step_in_hours * 60 * 60
        dates: list[tuple[str, str]] = []
        current_timestamp = start_date_float
        while current_timestamp <= end_date_float:
            dates.append(
                (
                    datetime.fromtimestamp(current_timestamp).isoformat(),
                    datetime.fromtimestamp(
                        current_timestamp + step_in_seconds).isoformat()
                )
            )
        return dates

    def save_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        if start_date is None:
            start_date_datetime = datetime.now() - timedelta(days=30)
            start_date = start_date_datetime.strftime(
                "%Y-%m-%dT00:00:00+01:00")
        if end_date is None:
            end_date_datetime = datetime.now() - timedelta(days=1)
            end_date = end_date_datetime.strftime("%Y-%m-%dT00:00:00+01:00")

        dates = self.slice_dates(start_date, end_date, 24)
        parts = []
        for (start, end) in tqdm(dates):
            result = self.fetch_data(
                start_date=start,
                end_date=end
            )
            parts.append(
                self.parse_result(result)
            )
        
        data = parts[0]
        for part in data[1:]:
            for production_type, values in part:
                for k in values.keys():
                    data[production_type][k] += values[k]

        for production_type, values in data.items():
            dataframe = pd.DataFrame(values)
            if not os.path.exists(self.save_dir):
                os.makedirs(self.save_dir)
            dataframe.to_csv(os.path.join(
                self.save_dir, f"{production_type}.csv"))
            self.logger.info(
                f"Saved {production_type} : {len(dataframe)} samples")

    def parse_result(self, result: dict[str, Any]) -> dict[str, Any]:
        if not result.get('success'):
            self.logger.error(f"Error  : {result.get('error')}")

        data = dict()

        for item in result['data']['actual_generations_per_production_type']:
            production_type = item.get('production_type')
            production_data = {
                'start': [],
                'end': [],
                'values': [],
            }

            for content in item['values']:
                # Append start date
                start = date_to_int(content['start_date'])
                production_data['start'].append(start)

                # Append end date
                end = date_to_int(content['end_date'])
                production_data['end'].append(end)

                # Append value
                production_data['values'].append(content['value'])

            data[production_type] = production_data
        return data
