import pandas as pd
import os
import logging
from datetime import datetime, timedelta
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def perform_quality_checks(df):
    """Performs various quality checks on the merged DataFrame and adds a data quality score."""
    logging.info("Performing data quality checks...")

    if df.empty:
        logging.warning("DataFrame is empty, skipping quality checks.")
        return df

    # 1. Missing data detection
    df['has_missing_data'] = df.isnull().any(axis=1)

    # 2. Outlier detection
    df['is_temp_outlier'] = ((df['tmax_f'] < -50) | (df['tmax_f'] > 130) | \
                             (df['tmin_f'] < -50) | (df['tmin_f'] > 130))
    df['is_demand_outlier'] = (df['demand_mwh'] < 0)
    df['is_outlier'] = df['is_temp_outlier'] | df['is_demand_outlier']

    # 3. Staleness check
    current_time = datetime.now() # Make current_time timezone-naive
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc']) # This should be timezone-naive from CSV
    df['is_stale'] = (current_time - df['timestamp_utc']) > timedelta(hours=48)

    # 4. Synchronization check
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    expected_cities = [city['name'] for city in config['cities']]
    df['all_cities_present'] = df.groupby('date')['city'].transform(lambda x: set(expected_cities).issubset(set(x)))

    # Calculate data quality score (0-100)
    quality_checks = ['has_missing_data', 'is_outlier', 'is_stale', 'all_cities_present']
    # Score is based on the percentage of passed checks (i.e., False values)
    df['data_quality_score'] = (1 - df[quality_checks].sum(axis=1) / len(quality_checks)) * 100

    logging.info("Data quality checks completed.")
    return df

def generate_quality_report(df):
    """Generates a summary report of data quality checks."""
    logging.info("Generating data quality report...")
    
    if df.empty:
        logging.warning("DataFrame is empty, generating empty quality report.")
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_rows": 0,
            "missing_data_rows": 0,
            "outlier_rows": 0,
            "stale_rows": 0,
            "incomplete_sync_days": 0,
            "average_quality_score": 0.0
        }
    else:
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_rows": len(df),
            "missing_data_rows": df['has_missing_data'].sum(),
            "outlier_rows": df['is_outlier'].sum(),
            "stale_rows": df['is_stale'].sum(),
            "incomplete_sync_days": len(df[~df['all_cities_present']]['date'].unique()),
            "average_quality_score": df['data_quality_score'].mean()
        }

    report_df = pd.DataFrame([report])
    return report_df

if __name__ == "__main__":
    print("This script is intended to be imported and used by pipeline.py.")