import requests
import yaml
import os
import logging
import time
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    """Loads the configuration from config.yaml."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_weather_data(city, date, api_key):
    """Fetches weather data for a given city and date from the NOAA API."""
    url = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&stationid={city['noaa_station_id']}&startdate={date}&enddate={date}&datatype=TMAX,TMIN&units=standard"
    headers = {'token': api_key}
    retries = 3
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            print(f"NOAA response for {city['name']} on {date}: {data}")  # Debug print
            if 'results' in data:
                return data['results']
            else:
                logging.warning(f"No weather data for {city['name']} on {date}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching weather data for {city['name']}: {e}")
            time.sleep(2 ** i)
    return None

def get_energy_data(city, date, api_key):
    """Fetches energy data for a given city and date from the EIA API."""
    url = f"https://api.eia.gov/v2/electricity/rto/region-data/data/?api_key={api_key}&frequency=daily&data[0]=D&facets[respondent][]={city['eia_region_code']}&start={date}&end={date}"
    retries = 3
    for i in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            print(f"EIA response for {city['name']} on {date}: {data}")  # Debug print
            if 'response' in data and 'data' in data['response'] and len(data['response']['data']) > 0:
                return data['response']['data']
            else:
                logging.warning(f"No energy data for {city['name']} on {date}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching energy data for {city['name']}: {e}")
            time.sleep(2 ** i)
    return None

def save_data(data, data_type, city_name, date):
    """Saves the fetched data to a JSON file."""
    raw_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    if not os.path.exists(raw_data_path):
        os.makedirs(raw_data_path)
    filename = f"{data_type}_{city_name.replace(' ', '_')}_{date.replace('-', '')}.json"
    filepath = os.path.join(raw_data_path, filename)
    with open(filepath, 'w') as f:
        f.write(str(data))
    logging.info(f"Saved {data_type} data for {city_name} on {date} to {filepath}")

