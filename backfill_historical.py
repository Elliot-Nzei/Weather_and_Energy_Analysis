import os
import sys
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from src.data_fetcher import load_config, get_weather_data, get_energy_data, save_data

def backfill_historical_data():
    """Fetches the last 90 days of historical data."""
    config = load_config()
    api_keys = {
        "noaa": config["noaa_token"],
        "eia": config["eia_api_key"]
    }
    
    today = datetime.now()
    for i in range(90):
        date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        for city in config["cities"]:
            # Fetch and save weather data
            weather_data = get_weather_data(city, date, api_keys["noaa"])
            if weather_data:
                save_data(weather_data, "weather", city['name'], date)

            # Fetch and save energy data
            energy_data = get_energy_data(city, date, api_keys["eia"])
            if energy_data:
                save_data(energy_data, "energy", city['name'], date)

if __name__ == "__main__":
    backfill_historical_data()
