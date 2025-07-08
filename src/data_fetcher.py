import requests
import yaml
import os
import logging
import time
import csv
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
    url = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&stationid={city['noaa_station_id']}&startdate={date}&enddate={date}&datatype=TMAX,TMIN,PRCP,SNOW,SNWD,AWND,TSUN,WDF2,WSF2&units=standard"
    headers = {'token': api_key}
    retries = 3
    for i in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            if 'results' in data:
                tmax, tmin, prcp, snow, snwd, awnd, tsun, wdf2, wsf2 = None, None, None, None, None, None, None, None, None
                for entry in data['results']:
                    if entry.get('datatype') == 'TMAX':
                        tmax = entry.get('value')
                    elif entry.get('datatype') == 'TMIN':
                        tmin = entry.get('value')
                    elif entry.get('datatype') == 'PRCP':
                        prcp = entry.get('value')
                    elif entry.get('datatype') == 'SNOW':
                        snow = entry.get('value')
                    elif entry.get('datatype') == 'SNWD':
                        snwd = entry.get('value')
                    elif entry.get('datatype') == 'AWND':
                        awnd = entry.get('value')
                    elif entry.get('datatype') == 'TSUN':
                        tsun = entry.get('value')
                    elif entry.get('datatype') == 'WDF2':
                        wdf2 = entry.get('value')
                    elif entry.get('datatype') == 'WSF2':
                        wsf2 = entry.get('value')
                return {
                    "date": date,
                    "city": city['name'],
                    "tmax_f": tmax,
                    "tmin_f": tmin,
                    "prcp": prcp,
                    "snow": snow,
                    "snwd": snwd,
                    "awnd": awnd,
                    "tsun": tsun,
                    "wdf2": wdf2,
                    "wsf2": wsf2,
                    "timestamp_utc": f"{date}T12:00:00Z"
                }
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
            if 'response' in data and 'data' in data['response'] and len(data['response']) > 0:
                return [{
                    "date": date,
                    "region": city['eia_region_code'],
                    "demand_mwh": entry.get('value'),
                    "timestamp_utc": f"{date}T12:00:00Z"
                } for entry in data['response']['data']]
            else:
                logging.warning(f"No energy data for {city['name']} on {date}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching energy data for {city['name']}: {e}")
            time.sleep(2 ** i)
    return None

def save_to_csv(data, data_type):
    """Appends data to a CSV file."""
    raw_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    if not os.path.exists(raw_data_path):
        os.makedirs(raw_data_path)

    filename = f"{data_type}_data.csv"
    filepath = os.path.join(raw_data_path, filename)
    file_exists = os.path.isfile(filepath)

    with open(filepath, 'a', newline='') as f:
        if data_type == 'weather':
            fieldnames = ['date', 'city', 'tmax_f', 'tmin_f', 'prcp', 'snow', 'snwd', 'awnd', 'tsun', 'wdf2', 'wsf2', 'timestamp_utc']
        elif data_type == 'energy':
            fieldnames = ['date', 'region', 'demand_mwh', 'timestamp_utc']
        else:
            return

        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        if isinstance(data, list):
            writer.writerows(data)
        else:
            writer.writerow(data)
    logging.info(f"Saved {data_type} data to {filepath}")
