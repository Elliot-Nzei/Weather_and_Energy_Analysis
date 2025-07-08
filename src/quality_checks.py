import pandas as pd
import os
import logging
from datetime import datetime, timedelta
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def perform_quality_checks(df):
    """Performs various quality checks on the merged DataFrame."""
    logging.info("Performing data quality checks...")

    # 1. Missing data detection
    missing_data = df.isnull().sum()
    logging.info(f"""Missing data:
{missing_data[missing_data > 0]}""")
    df['has_missing_data'] = df.isnull().any(axis=1)

    # 2. Outlier detection
    # Temperature outliers
    df['is_temp_outlier'] = ((df['tmax_f'] < -50) | (df['tmax_f'] > 130) | \
                             (df['tmin_f'] < -50) | (df['tmin_f'] > 130))
    # Negative demand outliers
    df['is_demand_outlier'] = (df['demand_mwh'] < 0)
    df['is_outlier'] = df['is_temp_outlier'] | df['is_demand_outlier']

    # 3. Staleness check: Flag records older than 48 hours
    # Assuming 'timestamp_utc' is in ISO format and represents the data collection time
    # For this check, we'll assume the current run time as the reference.
    # In a real-time system, this would compare against the actual data timestamp.
    current_time = datetime.utcnow()
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    df['is_stale'] = (current_time - df['timestamp_utc']) > timedelta(hours=48)

    # 4. Synchronization: Ensure each city has data for the same date
    # This check is implicitly handled by the inner merge in data_processor.py
    # If a city doesn't have data for a date, it won't be in the merged_df for that date.
    # We can add a check to see if all expected cities are present for each date.
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    expected_cities = [city['name'] for city in config['cities']]
    
    # Group by date and check if all expected cities are present
    df['all_cities_present'] = df.groupby('date')['city'].transform(lambda x: set(expected_cities).issubset(set(x)))

    logging.info("Data quality checks completed.")
    return df

if __name__ == "__main__":
    # Example usage (assuming a merged_df is available)
    # This part is for testing the quality checks independently
    # In the actual pipeline, this will be called by pipeline.py
    print("This script is intended to be imported and used by pipeline.py.")
    print("Run pipeline.py to see quality checks in action.")
