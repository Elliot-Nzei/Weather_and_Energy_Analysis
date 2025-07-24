import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from data_fetcher import load_config, get_weather_data, get_energy_data, save_to_csv
from data_processor import process_data
from quality_checks import perform_quality_checks
from analysis import analyze_data

FAILED_FETCHES_FILE = os.path.join(os.path.dirname(__file__), 'data', 'raw_responses', 'failed_fetches.json')

def load_failed_fetches():
    if os.path.exists(FAILED_FETCHES_FILE):
        with open(FAILED_FETCHES_FILE, 'r') as f:
            # Convert list of lists back to set of tuples
            return set(tuple(item) for item in json.load(f))
    return set()

def save_failed_fetches(failed_fetches):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(FAILED_FETCHES_FILE), exist_ok=True)
    with open(FAILED_FETCHES_FILE, 'w') as f:
        # Convert set of tuples to list of lists for JSON serialization
        json.dump(list(failed_fetches), f)

def clear_failed_fetches():
    if os.path.exists(FAILED_FETCHES_FILE):
        os.remove(FAILED_FETCHES_FILE)
        logging.info(f"Cleared failed fetches file: {FAILED_FETCHES_FILE}")

def backfill_historical_data():
    """Fetches the last 90 days of historical data, processes it, performs quality checks, and statistical analysis."""
    config = load_config()
    api_keys = {
        "noaa": config["noaa_token"],
        "eia": config["eia_api_key"]
    }
    
    # Clear existing CSVs to ensure fresh data for processing
    raw_data_path = os.path.join(os.path.dirname(__file__), 'data', 'raw')
    weather_csv_path = os.path.join(raw_data_path, 'weather_data.csv')
    energy_csv_path = os.path.join(raw_data_path, 'energy_data.csv')
    if os.path.exists(weather_csv_path):
        os.remove(weather_csv_path)
    if os.path.exists(energy_csv_path):
        os.remove(energy_csv_path)

    failed_fetches = load_failed_fetches()

    today = datetime.now()
    for i in range(90):
        date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        for city in config["cities"]:
            city_name = city['name']
            
            # Check and fetch weather data
            weather_key = (city_name, date, "weather")
            if weather_key in failed_fetches:
                logging.info(f"Skipping weather data for {city_name} on {date} due to previous failure.")
            else:
                weather_data = get_weather_data(city, date, api_keys["noaa"])
                if weather_data:
                    save_to_csv(weather_data, "weather")
                else:
                    failed_fetches.add(weather_key)

            # Check and fetch energy data
            energy_key = (city_name, date, "energy")
            if energy_key in failed_fetches:
                logging.info(f"Skipping energy data for {city_name} on {date} due to previous failure.")
            else:
                energy_data = get_energy_data(city, date, api_keys["eia"])
                if energy_data:
                    save_to_csv(energy_data, "energy")
                else:
                    failed_fetches.add(energy_key)
    
    save_failed_fetches(failed_fetches)

    # Process and merge data
    merged_df = process_data()

    # Perform quality checks
    if merged_df is not None:
        df_with_quality_flags = perform_quality_checks(merged_df)
        
        # Save the DataFrame with quality flags to processed directory
        processed_data_path = os.path.join(os.path.dirname(__file__), 'data', 'processed')
        output_filename = f"merged_with_quality_flags_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet"
        output_filepath = os.path.join(processed_data_path, output_filename)
        df_with_quality_flags.to_parquet(output_filepath, index=False)
        logging.info(f"Merged data with quality flags saved to {output_filepath}")

        # Perform statistical analysis
        analyze_data()

def backfill_weather_only():
    """Fetches the last 90 days of weather data for configured cities and saves to CSV."""
    config = load_config()
    api_keys = {
        "noaa": config["noaa_token"]
    }
    raw_data_path = os.path.join(os.path.dirname(__file__), 'data', 'raw')
    weather_csv_path = os.path.join(raw_data_path, 'weather_data.csv')
    if os.path.exists(weather_csv_path):
        os.remove(weather_csv_path)
    
    failed_fetches = load_failed_fetches()

    today = datetime.now()
    for i in range(90):
        date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        for city in config["cities"]:
            city_name = city['name']
            weather_key = (city_name, date, "weather")
            if weather_key in failed_fetches:
                logging.info(f"Skipping weather data for {city_name} on {date} due to previous failure.")
            else:
                weather_data = get_weather_data(city, date, api_keys["noaa"])
                if weather_data:
                    save_to_csv(weather_data, "weather")
                else:
                    failed_fetches.add(weather_key)
    save_failed_fetches(failed_fetches)

def backfill_energy_only():
    """Fetches the last 90 days of energy data for configured cities and saves to CSV."""
    config = load_config()
    api_keys = {
        "eia": config["eia_api_key"]
    }
    raw_data_path = os.path.join(os.path.dirname(__file__), 'data', 'raw')
    energy_csv_path = os.path.join(raw_data_path, 'energy_data.csv')
    if os.path.exists(energy_csv_path):
        os.remove(energy_csv_path)
    
    failed_fetches = load_failed_fetches()

    today = datetime.now()
    for i in range(90):
        date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        for city in config["cities"]:
            city_name = city['name']
            energy_key = (city_name, date, "energy")
            if energy_key in failed_fetches:
                logging.info(f"Skipping energy data for {city_name} on {date} due to previous failure.")
            else:
                energy_data = get_energy_data(city, date, api_keys["eia"])
                if energy_data:
                    save_to_csv(energy_data, "energy")
                else:
                    failed_fetches.add(energy_key)
    save_failed_fetches(failed_fetches)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == '--weather-only':
            backfill_weather_only()
        elif sys.argv[1] == '--energy-only':
            backfill_energy_only()
        elif sys.argv[1] == '--clear-failed-fetches':
            clear_failed_fetches()
        else:
            backfill_historical_data()
    else:
        backfill_historical_data()