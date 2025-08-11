

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import json
from datetime import datetime, date

# Set page config
st.set_page_config(layout="wide", page_title="Weather and Energy Analysis", page_icon="⚡")

# --- Helper Functions to Load Data ---
@st.cache_data
def load_all_data():
    analytics_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'analytics')
    processed_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')

    # Load merged data with quality flags
    df = pd.DataFrame()
    processed_files = [f for f in os.listdir(processed_path) if f.startswith('merged_with_quality_flags_') and f.endswith('.parquet')]
    if processed_files:
        latest_processed_file = max(processed_files, key=lambda f: os.path.getmtime(os.path.join(processed_path, f)))
        df = pd.read_parquet(os.path.join(processed_path, latest_processed_file))
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        df = df.dropna(subset=['date'])

    # Load analytics data
    def load_json_file(filename):
        filepath = os.path.join(analytics_path, filename)
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return json.load(f)
        return {}

    correlations = load_json_file('correlations.json')
    summary_stats = load_json_file('summary_stats.json')
    top_cities_by_demand = load_json_file('top_cities_by_demand.json')

    # Load parquet data
    def load_parquet_file(filename):
        filepath = os.path.join(analytics_path, filename)
        if os.path.exists(filepath):
            return pd.read_parquet(filepath)
        return pd.DataFrame()

    timeseries_df = load_parquet_file('timeseries.parquet')
    if not timeseries_df.empty:
        timeseries_df['date'] = pd.to_datetime(timeseries_df.index, errors='coerce').date
        timeseries_df = timeseries_df.dropna(subset=['date'])

    heatmap_df = load_parquet_file('heatmap.parquet')

    return df, correlations, timeseries_df, heatmap_df, summary_stats, top_cities_by_demand

# --- Initialize Session State ---
if 'data_loaded' not in st.session_state:
    st.session_state.df, st.session_state.correlations, st.session_state.timeseries_df, \
    st.session_state.heatmap_df, st.session_state.summary_stats, st.session_state.top_cities_by_demand = load_all_data()
    st.session_state.data_loaded = True

# --- Dashboard Layout ---
st.markdown("<h1 style='text-align: center; color: #2c3e50;'>⚡ US Weather and Energy Analysis Dashboard ⚡</h1>", unsafe_allow_html=True)
st.markdown("---_---")

# --- Sidebar for filters ---
with st.sidebar:
    st.image("https://www.eia.gov/todayinenergy/images/2017.03.31/main.png", use_container_width=True)
    st.markdown("<h2 style='text-align: center;'>Filters</h2>", unsafe_allow_html=True)

    # Date Range Filter
    if not st.session_state.df.empty:
        min_date = st.session_state.df['date'].min()
        max_date = st.session_state.df['date'].max()
        date_range = st.date_input("Select Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    else:
        date_range = st.date_input("Select Date Range", value=(date(2023, 1, 1), date.today()))

    # City Multiselect Filter
    all_cities = st.session_state.df['city'].unique().tolist() if not st.session_state.df.empty else []
    selected_cities = st.multiselect("Select Cities", all_cities, default=all_cities)

# --- Filter Data based on Selection ---
if st.session_state.data_loaded and not st.session_state.df.empty:
    if len(date_range) == 2:
        filtered_df = st.session_state.df[(st.session_state.df['date'] >= date_range[0]) & (st.session_state.df['date'] <= date_range[1])]
        filtered_timeseries_df = st.session_state.timeseries_df[(st.session_state.timeseries_df['date'] >= date_range[0]) & (st.session_state.timeseries_df['date'] <= date_range[1])]
    else:
        filtered_df = st.session_state.df
        filtered_timeseries_df = st.session_state.timeseries_df

    if selected_cities:
        filtered_df = filtered_df[filtered_df['city'].isin(selected_cities)]
        filtered_timeseries_df = filtered_timeseries_df[filtered_timeseries_df['city'].isin(selected_cities)]
else:
    st.warning("No data available to display. Please run the data pipeline first by running `make backfill`.")
    filtered_df = pd.DataFrame()
    filtered_timeseries_df = pd.DataFrame()

# --- Display Sections ---
if not filtered_df.empty:
    # --- Key Metrics ---
    st.markdown("###  KPIs")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Avg. Demand (MWh)", f"{filtered_df['demand_mwh'].mean():,.0f}")
    with col2:
        st.metric("Avg. Max Temp (°F)", f"{filtered_df['tmax_f'].mean():.1f}°F")
    with col3:
        st.metric("Correlation (Tmax vs Demand)", f"{filtered_df.groupby('city').apply(lambda x: x['tmax_f'].corr(x['demand_mwh'])).mean():.2f}")
    with col4:
        st.metric("Total Records", f"{len(filtered_df):,}")

    st.markdown("---_---")

    # --- Tabbed Layout for Visualizations ---
    tab1, tab2, tab3, tab4 = st.tabs(["Time Series Analysis", "Correlation Analysis", "Heatmap", "Data Quality"])

    with tab1:
        st.markdown("### 📈 Time Series Analysis")
        if not filtered_timeseries_df.empty:
            fig_ts = px.line(filtered_timeseries_df, x='date', y='demand_mwh', color='city', title='Energy Demand Over Time')
            st.plotly_chart(fig_ts, use_container_width=True)

            fig_temp = px.line(filtered_timeseries_df, x='date', y='tmax_f', color='city', title='Max Temperature Over Time')
            st.plotly_chart(fig_temp, use_container_width=True)
        else:
            st.info("No time series data to display for the selected filters.")

    with tab2:
        st.markdown("### 🔗 Correlation Analysis")
        if st.session_state.correlations:
            corr_df = pd.DataFrame.from_dict(st.session_state.correlations, orient='index')
            st.dataframe(corr_df)

            st.markdown("#### Scatterplot: Temperature vs. Energy Demand")
            fig_scatter = px.scatter(filtered_df, x='tmax_f', y='demand_mwh', color='city', hover_data=['date', 'tmin_f'], title='Temperature vs. Energy Demand by City')
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.info("No correlation data available.")

    with tab3:
        st.markdown("### 🔥 Heatmap: Average Demand by Temperature Range and Day Type")
        if not st.session_state.heatmap_df.empty:
            city_heatmap = st.selectbox("Select City for Heatmap", selected_cities)
            if city_heatmap:
                city_heatmap_df = st.session_state.heatmap_df.loc[city_heatmap]
                fig_heatmap = px.imshow(city_heatmap_df, labels=dict(x="Day Type", y="Temperature Range", color="Average Demand (MWh)"), title=f'Average Energy Demand for {city_heatmap}')
                st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("No heatmap data available.")

    with tab4:
        st.markdown("### ✅ Data Quality")
        st.dataframe(filtered_df.describe())
        st.markdown("#### Data Quality Score")
        st.dataframe(filtered_df[['city', 'data_quality_score']].groupby('city').mean())

else:
    st.info("Select filters to view data.")

# --- Footer ---
st.markdown("---_---")
st.markdown("<p style='text-align: center; color: grey;'>Developed by Elliot Nzei</p>", unsafe_allow_html=True)
