import os
import sys
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_fetcher import load_config, get_weather_data, get_energy_data, save_to_csv

def run_pipeline():
    """Runs the data collection pipeline."""
    config = load_config()
    api_keys = {
        "noaa": config["noaa_token"],
        "eia": config["eia_api_key"]
    }
    
    # For now, we'll fetch data for yesterday.
    # In a production environment, this would be triggered by a scheduler.
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    for city in config["cities"]:
        # Fetch and save weather data
        weather_data = get_weather_data(city, yesterday, api_keys["noaa"])
        if weather_data:
            save_to_csv(weather_data, "weather")

        # Fetch and save energy data
        energy_data = get_energy_data(city, yesterday, api_keys["eia"])
        if energy_data:
            save_to_csv(energy_data, "energy")

if __name__ == "__main__":
    run_pipeline()