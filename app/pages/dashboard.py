# app/pages/dashboard.py
import streamlit as st

from app.utils import load_data, format_kph_to_pace
from app.plots import plot_distance_over_time, plot_pace_hist, plot_monthly_distance
from src.analysis.constants import CURRENT_YEAR

def run():
    st.title("🏃‍♂️ Strava Dashboard")

    df = load_data()
    sport_types = ["Walk", "Run", "Ride"]
    df = df[df["sport_type"].isin(sport_types)]

    # Sidebar filters
    st.sidebar.header("Filters")
    years = sorted(df["year"].unique(), reverse=True)
    selected_year = st.sidebar.multiselect("Select year", years, default=[CURRENT_YEAR])

    sports = df["sport_type"].unique()
    selected_sport = st.sidebar.radio("Sport Type", options=sports, index=0)

    # Month filter
    available_months_df = df[df["year"].isin(selected_year)]
    month_options = sorted(
        available_months_df["month"].unique(),
        key=lambda x: ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"].index(x)
    )
    selected_month = st.sidebar.multiselect("Month", month_options, default=[])

    # Apply filters
    if not selected_year:
        selected_year = df["year"].unique()
    filtered = df[
        df["year"].isin(selected_year) & 
        (df["sport_type"] == selected_sport)
    ]
    if selected_month:
        filtered = filtered[filtered["month"].isin(selected_month)]

    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    if not filtered.empty:
        col1.metric("Total Distance", f"{filtered['distance'].sum():.1f} km")
        col2.metric("Total Time", f"{filtered['duration'].sum()/60:.1f} hrs")
        col3.metric("Average Speed", f"{filtered['average_speed'].mean():.2f} km/h")
        col4.metric("Average Pace", f"{format_kph_to_pace(filtered['average_speed'].mean())} min/km")
    else:
        col1.metric("Total Distance", "0 km")
        col2.metric("Total Time", "0 hrs")
        col3.metric("Average Speed", "0 km/h")
        col4.metric("Average Pace", "N/A")

    # Plots
    if not filtered.empty:
        st.plotly_chart(plot_distance_over_time(filtered), use_container_width=True)
        st.plotly_chart(plot_pace_hist(filtered), use_container_width=True)
        st.plotly_chart(plot_monthly_distance(filtered), use_container_width=True)
