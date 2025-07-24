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
    """Reads raw weather and energy data, processes it, and returns the merged DataFrame.
    """
    raw_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    
    weather_filepath = os.path.join(raw_data_path, 'weather_data.csv')
    energy_filepath = os.path.join(raw_data_path, 'energy_data.csv')

    if not os.path.exists(weather_filepath) or not os.path.exists(energy_filepath):
        logging.error("Raw weather_data.csv or energy_data.csv not found. Please run data collection first.")
        return pd.DataFrame() # Return empty DataFrame instead of None

    try:
        weather_df = pd.read_csv(weather_filepath)
        if weather_df.empty:
            logging.warning("weather_data.csv is empty. Creating an empty DataFrame with expected columns.")
            # Define expected columns for weather_df if it's empty
            weather_df = pd.DataFrame(columns=['date', 'city', 'tmax_f', 'tmin_f', 'prcp', 'snow', 'snwd', 'awnd', 'tsun', 'wdf2', 'wsf2', 'timestamp_utc'])
        else:
            # Convert date columns to datetime objects
            weather_df['date'] = pd.to_datetime(weather_df['date'])
            # Convert temperature from tenths of Celsius to Fahrenheit if necessary
            if 'tmax_f' in weather_df.columns and weather_df['tmax_f'].dtype != 'float64':
                weather_df['tmax_f'] = (weather_df['tmax_f'] / 10 * 9/5) + 32
            if 'tmin_f' in weather_df.columns and weather_df['tmin_f'].dtype != 'float64':
                weather_df['tmin_f'] = (weather_df['tmin_f'] / 10 * 9/5) + 32

    except pd.errors.EmptyDataError:
        logging.warning("weather_data.csv is empty. Creating an empty DataFrame with expected columns.")
        weather_df = pd.DataFrame(columns=['date', 'city', 'tmax_f', 'tmin_f', 'prcp', 'snow', 'snwd', 'awnd', 'tsun', 'wdf2', 'wsf2', 'timestamp_utc'])
    except Exception as e:
        logging.error(f"Error reading weather_data.csv: {e}")
        return pd.DataFrame()

    try:
        energy_df = pd.read_csv(energy_filepath)
        if energy_df.empty:
            logging.warning("energy_data.csv is empty. Returning empty DataFrame.")
            return pd.DataFrame()
        else:
            energy_df['date'] = pd.to_datetime(energy_df['date'])
    except pd.errors.EmptyDataError:
        logging.warning("energy_data.csv is empty. Returning empty DataFrame.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error reading energy_data.csv: {e}")
        return pd.DataFrame()

    # Aggregate hourly energy data to daily total
    daily_energy_df = energy_df.groupby(['date', 'city', 'region'])['demand_mwh'].sum().reset_index()

    # Merge dataframes
    merged_df = pd.merge(weather_df, daily_energy_df, on=['date', 'city'], how='inner')

    logging.info("Processed and merged data.")
    return merged_df

if __name__ == "__main__":
    df = process_data()
    if df is not None:
        print(df.head())
