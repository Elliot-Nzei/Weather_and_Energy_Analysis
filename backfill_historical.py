import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from data_fetcher import load_config, get_weather_data, get_energy_data, save_to_csv
from data_processor import process_data
from quality_checks import perform_quality_checks
from analysis import analyze_data

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

    today = datetime.now()
    for i in range(90):
        date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        for city in config["cities"]:
            # Fetch and save weather data
            weather_data = get_weather_data(city, date, api_keys["noaa"])
            if weather_data:
                save_to_csv(weather_data, "weather")

            # Fetch and save energy data
            energy_data = get_energy_data(city, date, api_keys["eia"])
            if energy_data:
                save_to_csv(energy_data, "energy")

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

if __name__ == "__main__":
    backfill_historical_data()