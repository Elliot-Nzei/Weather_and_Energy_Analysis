import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import logging

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_fetcher import load_config, get_weather_data, get_energy_data, save_to_csv
from data_processor import process_data
from quality_checks import perform_quality_checks, generate_quality_report
from analysis import analyze_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    """Runs the full data pipeline: fetch, process, quality check, and analyze."""
    config = load_config()
    api_keys = {
        "noaa": config["noaa_token"],
        "eia": config["eia_api_key"]
    }
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Clear old raw data files
    raw_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    weather_csv_path = os.path.join(raw_data_path, 'weather_data.csv')
    energy_csv_path = os.path.join(raw_data_path, 'energy_data.csv')
    if os.path.exists(weather_csv_path):
        os.remove(weather_csv_path)
    if os.path.exists(energy_csv_path):
        os.remove(energy_csv_path)

    # Create empty weather data file with headers
    save_to_csv(None, "weather")

    # Fetch new data
    for city in config["cities"]:
        weather_data = get_weather_data(city, yesterday, api_keys["noaa"])
        if weather_data:
            save_to_csv(weather_data, "weather")

        energy_data = get_energy_data(city, yesterday, api_keys["eia"])
        if energy_data:
            save_to_csv(energy_data, "energy")

    # Process and perform quality checks
    merged_df = process_data()
    
    # Always perform quality checks and generate report, even if merged_df is empty
    df_with_quality = perform_quality_checks(merged_df)

    # Generate and save quality report
    quality_report_df = generate_quality_report(df_with_quality)
    processed_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    report_filename = f"quality_report_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
    report_filepath = os.path.join(processed_data_path, report_filename)
    quality_report_df.to_csv(report_filepath, index=False)
    logging.info(f"Data quality report saved to {report_filepath}")

    if not merged_df.empty:
        # Save final data
        final_df = df_with_quality[['date', 'city', 'tmax_f', 'tmin_f', 'demand_mwh', 'is_outlier', 'data_quality_score']]
        output_filename = f"merged_{pd.Timestamp.now().strftime('%Y%m%d')}.parquet"
        output_filepath = os.path.join(processed_data_path, output_filename)
        final_df.to_parquet(output_filepath, index=False)
        logging.info(f"Final processed data saved to {output_filepath}")

        # Perform analysis
        analyze_data()
    else:
        logging.warning("No data available for further processing or analysis.")

if __name__ == "__main__":
    run_pipeline()
