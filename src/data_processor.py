import pandas as pd
import os
import logging
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    """Loads the configuration from config.yaml."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def process_data():
    """Reads raw weather and energy data, processes it, and saves the merged data.
    """
    raw_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    processed_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    
    if not os.path.exists(processed_data_path):
        os.makedirs(processed_data_path)

    weather_filepath = os.path.join(raw_data_path, 'weather_data.csv')
    energy_filepath = os.path.join(raw_data_path, 'energy_data.csv')

    if not os.path.exists(weather_filepath) or not os.path.exists(energy_filepath):
        logging.error("Raw weather_data.csv or energy_data.csv not found. Please run data collection first.")
        return

    weather_df = pd.read_csv(weather_filepath)
    energy_df = pd.read_csv(energy_filepath)

    # Convert date columns to datetime objects
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    energy_df['date'] = pd.to_datetime(energy_df['date'])

    # Convert temperature from tenths of Celsius to Fahrenheit if necessary
    # Assuming TMAX and TMIN are in tenths of Celsius based on project plan
    # NOAA API with units=standard returns values in station's native units, often Celsius in tenths
    if 'tmax_f' in weather_df.columns and weather_df['tmax_f'].dtype != 'float64': # Check if conversion is needed
        weather_df['tmax_f'] = (weather_df['tmax_f'] / 10 * 9/5) + 32
    if 'tmin_f' in weather_df.columns and weather_df['tmin_f'].dtype != 'float64': # Check if conversion is needed
        weather_df['tmin_f'] = (weather_df['tmin_f'] / 10 * 9/5) + 32

    # Map city names to EIA region codes for merging
    config = load_config()
    city_to_region_map = {city['name']: city['eia_region_code'] for city in config['cities']}
    weather_df['region'] = weather_df['city'].map(city_to_region_map)

    # Merge dataframes
    merged_df = pd.merge(weather_df, energy_df, on=['date', 'region'], how='inner')

    # Save to parquet
    output_filename = f"merged_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet"
    output_filepath = os.path.join(processed_data_path, output_filename)
    merged_df.to_parquet(output_filepath, index=False)
    logging.info(f"Processed and merged data saved to {output_filepath}")

if __name__ == "__main__":
    process_data()
