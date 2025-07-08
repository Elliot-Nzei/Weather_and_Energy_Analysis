import pandas as pd
import os
import logging
import json
from scipy.stats import pearsonr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def analyze_data():
    """Performs statistical analysis on the merged and quality-checked data."""
    processed_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    analytics_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'analytics')
    
    if not os.path.exists(analytics_data_path):
        os.makedirs(analytics_data_path)

    # Find the latest processed parquet file
    processed_files = [f for f in os.listdir(processed_data_path) if f.startswith('merged_with_quality_flags_') and f.endswith('.parquet')]
    if not processed_files:
        logging.error("No processed data found. Please run the pipeline first.")
        return
    latest_processed_file = max(processed_files, key=lambda f: os.path.getmtime(os.path.join(processed_data_path, f)))
    df = pd.read_parquet(os.path.join(processed_data_path, latest_processed_file))

    logging.info("Starting statistical analysis...")

    # Ensure date column is datetime and set as index for time series analysis
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()

    # --- Correlation Analysis (Temperature vs. Demand) ---
    correlations = {}
    for city in df['city'].unique():
        city_df = df[df['city'] == city].dropna(subset=['tmax_f', 'demand_mwh'])
        if len(city_df) > 1:
            corr, _ = pearsonr(city_df['tmax_f'], city_df['demand_mwh'])
            r_squared = corr**2
            correlations[city] = {
                "pearson_correlation": corr,
                "r_squared": r_squared
            }
    
    correlations_filepath = os.path.join(analytics_data_path, 'correlations.json')
    with open(correlations_filepath, 'w') as f:
        json.dump(correlations, f, indent=2)
    logging.info(f"Correlation analysis saved to {correlations_filepath}")

    # --- Temporal Trend Analysis (Weekly and Seasonal) ---
    # For time series plotting, we'll save the main DataFrame (or a subset)
    # Ensure all necessary columns are present for plotting
    timeseries_df = df[['city', 'tmax_f', 'tmin_f', 'demand_mwh']].copy()
    timeseries_df['day_of_week'] = timeseries_df.index.dayofweek # Monday=0, Sunday=6
    timeseries_df['month'] = timeseries_df.index.month
    timeseries_df['year'] = timeseries_df.index.year

    timeseries_filepath = os.path.join(analytics_data_path, 'timeseries.parquet')
    timeseries_df.to_parquet(timeseries_filepath, index=True)
    logging.info(f"Time series data saved to {timeseries_filepath}")

    # --- Heatmap Dataset Preparation (Average usage grouped by temp range and day) ---
    # Define temperature ranges
    temp_bins = [-float('inf'), 50, 60, 70, 80, 90, float('inf')]
    temp_labels = ['<50°F', '50-60°F', '60-70°F', '70-80°F', '80-90°F', '>90°F']
    df['temp_range'] = pd.cut(df['tmax_f'], bins=temp_bins, labels=temp_labels, right=False)

    # Group by temp_range and day of week (weekday/weekend)
    df['day_type'] = df.index.dayofweek.map(lambda x: 'Weekend' if x >= 5 else 'Weekday')
    heatmap_data = df.groupby(['city', 'temp_range', 'day_type'])['demand_mwh'].mean().unstack(fill_value=0)

    heatmap_filepath = os.path.join(analytics_data_path, 'heatmap.parquet')
    heatmap_data.to_parquet(heatmap_filepath, index=True)
    logging.info(f"Heatmap data saved to {heatmap_filepath}")

    # --- Calculated insights and descriptive statistics ---
    summary_stats = {
        "overall_demand_mwh_mean": df['demand_mwh'].mean(),
        "overall_demand_mwh_std": df['demand_mwh'].std(),
        "overall_tmax_f_mean": df['tmax_f'].mean(),
        "overall_tmin_f_mean": df['tmin_f'].mean(),
        "demand_by_city": df.groupby('city')['demand_mwh'].mean().to_dict(),
        "demand_weekday_vs_weekend": df.groupby('day_type')['demand_mwh'].mean().to_dict()
    }

    summary_stats_filepath = os.path.join(analytics_data_path, 'summary_stats.json')
    with open(summary_stats_filepath, 'w') as f:
        json.dump(summary_stats, f, indent=2)
    logging.info(f"Summary statistics saved to {summary_stats_filepath}")

    logging.info("Statistical analysis completed.")

if __name__ == "__main__":
    analyze_data()
