import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json

# Set page config
st.set_page_config(layout="wide", page_title="Weather and Energy Analysis")

# --- Helper Functions to Load Data ---
@st.cache_data
def load_data():
    analytics_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'analytics')
    processed_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')

    # Load merged data with quality flags
    processed_files = [f for f in os.listdir(processed_path) if f.startswith('merged_with_quality_flags_') and f.endswith('.parquet')]
    if processed_files:
        latest_processed_file = max(processed_files, key=lambda f: os.path.getmtime(os.path.join(processed_path, f)))
        df = pd.read_parquet(os.path.join(processed_path, latest_processed_file))
        df['date'] = pd.to_datetime(df['date'])
    else:
        df = pd.DataFrame()

    # Load analytics data
    correlations = {}
    correlations_filepath = os.path.join(analytics_path, 'correlations.json')
    if os.path.exists(correlations_filepath):
        with open(correlations_filepath, 'r') as f:
            correlations = json.load(f)

    timeseries_df = pd.DataFrame()
    timeseries_filepath = os.path.join(analytics_path, 'timeseries.parquet')
    if os.path.exists(timeseries_filepath):
        timeseries_df = pd.read_parquet(timeseries_filepath)
        timeseries_df['date'] = pd.to_datetime(timeseries_df.index)

    heatmap_df = pd.DataFrame()
    heatmap_filepath = os.path.join(analytics_path, 'heatmap.parquet')
    if os.path.exists(heatmap_filepath):
        heatmap_df = pd.read_parquet(heatmap_filepath)

    summary_stats = {}
    summary_stats_filepath = os.path.join(analytics_path, 'summary_stats.json')
    if os.path.exists(summary_stats_filepath):
        with open(summary_stats_filepath, 'r') as f:
            summary_stats = json.load(f)

    top_cities_by_demand = {}
    top_cities_filepath = os.path.join(analytics_path, 'top_cities_by_demand.json')
    if os.path.exists(top_cities_filepath):
        with open(top_cities_filepath, 'r') as f:
            top_cities_by_demand = json.load(f)

    return df, correlations, timeseries_df, heatmap_df, summary_stats, top_cities_by_demand

df, correlations, timeseries_df, heatmap_df, summary_stats, top_cities_by_demand = load_data()

# --- Dashboard Layout ---
st.title("US Weather and Energy Analysis Dashboard")

# Sidebar for filters
st.sidebar.header("Filters")

# Date Range Filter
if not df.empty:
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()
    date_range = st.sidebar.date_input("Select Date Range", 
                                       value=(min_date, max_date),
                                       min_value=min_date,
                                       max_value=max_date)
    if len(date_range) == 2:
        filtered_df = df[(df['date'].dt.date >= date_range[0]) & (df['date'].dt.date <= date_range[1])]
        filtered_timeseries_df = timeseries_df[(timeseries_df['date'].dt.date >= date_range[0]) & (timeseries_df['date'].dt.date <= date_range[1])]
    else:
        filtered_df = df
        filtered_timeseries_df = timeseries_df
else:
    st.warning("No data available to display. Please run the data pipeline.")
    filtered_df = pd.DataFrame()
    filtered_timeseries_df = pd.DataFrame()

# City Multiselect Filter
all_cities = df['city'].unique().tolist() if not df.empty else []
selected_cities = st.sidebar.multiselect("Select Cities", all_cities, default=all_cities)

if not filtered_df.empty:
    filtered_df = filtered_df[filtered_df['city'].isin(selected_cities)]
    filtered_timeseries_df = filtered_timeseries_df[filtered_timeseries_df['city'].isin(selected_cities)]

# --- Display Sections ---

# 1. Geographic Overview (Placeholder - requires geographical data/coordinates)
st.header("1. Geographic Overview")
if not filtered_df.empty:
    st.write("Interactive US Map (Placeholder - requires city coordinates)")
    # Example: Display a table of current data for selected cities
    st.dataframe(filtered_df[['date', 'city', 'tmax_f', 'demand_mwh']].tail(5))
else:
    st.info("No data to display for Geographic Overview.")

# 2. Time Series Analysis
st.header("2. Time Series Analysis")
if not filtered_timeseries_df.empty:
    city_for_timeseries = st.selectbox("Select City for Time Series", selected_cities)
    if city_for_timeseries:
        city_ts_df = filtered_timeseries_df[filtered_timeseries_df['city'] == city_for_timeseries]
        if not city_ts_df.empty:
            fig_ts = go.Figure()
            fig_ts.add_trace(go.Scatter(x=city_ts_df['date'], y=city_ts_df['tmax_f'], mode='lines', name='Max Temp (°F)', yaxis='y1'))
            fig_ts.add_trace(go.Scatter(x=city_ts_df['date'], y=city_ts_df['demand_mwh'], mode='lines', name='Demand (MWh)', yaxis='y2'))
            fig_ts.update_layout(
                title=f'Temperature and Energy Demand for {city_for_timeseries}',
                xaxis_title='Date',
                yaxis_title='Max Temp (°F)',
                yaxis2=dict(title='Demand (MWh)', overlaying='y', side='right'),
                hovermode='x unified'
            )
            st.plotly_chart(fig_ts, use_container_width=True)
        else:
            st.info(f"No time series data for {city_for_timeseries} in the selected date range.")
else:
    st.info("No data to display for Time Series Analysis.")

# 3. Correlation Analysis
st.header("3. Correlation Analysis")
if correlations:
    st.write("Pearson Correlation and R-squared between Max Temperature and Energy Demand:")
    corr_df = pd.DataFrame.from_dict(correlations, orient='index')
    st.dataframe(corr_df)

    if not filtered_df.empty:
        st.subheader("Scatterplot: Temperature vs. Energy Demand")
        fig_scatter = px.scatter(filtered_df, x='tmax_f', y='demand_mwh', color='city',
                                 hover_data=['date', 'tmin_f'],
                                 title='Temperature vs. Energy Demand by City')
        st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.info("No correlation data available. Please run the analysis pipeline.")

# 4. Heatmap
st.header("4. Heatmap: Average Demand by Temperature Range and Day Type")
if not heatmap_df.empty:
    selected_city_heatmap = st.selectbox("Select City for Heatmap", heatmap_df.index.get_level_values('city').unique())
    if selected_city_heatmap:
        city_heatmap_df = heatmap_df.loc[selected_city_heatmap]
        fig_heatmap = px.imshow(city_heatmap_df,
                                 labels=dict(x="Day Type", y="Temperature Range", color="Average Demand (MWh)"),
                                 x=city_heatmap_df.columns,
                                 y=city_heatmap_df.index,
                                 title=f'Average Energy Demand for {selected_city_heatmap}')
        st.plotly_chart(fig_heatmap, use_container_width=True)
else:
    st.info("No heatmap data available. Please run the analysis pipeline.")

# 5. Top Cities by Energy Consumption
st.header("5. Top Cities by Energy Consumption")
if top_cities_by_demand:
    top_cities_df = pd.DataFrame(top_cities_by_demand.items(), columns=['City', 'Average Demand (MWh)'])
    top_cities_df = top_cities_df.sort_values(by='Average Demand (MWh)', ascending=False)
    st.dataframe(top_cities_df)
else:
    st.info("No top cities by energy consumption data available. Please run the analysis pipeline.")

# Summary Statistics
st.header("Summary Statistics")
if summary_stats:
    st.json(summary_stats)
else:
    st.info("No summary statistics available.")

# Instructions to run
st.sidebar.markdown("""
---
### How to Run
1. Ensure you have run `make backfill` to generate data.
2. Run `streamlit run dashboards/app.py` in your terminal.
""")