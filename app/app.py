# app/app.py
import sys
import os
import subprocess
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.express as px
from loguru import logger

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.plots import (
    safe_plotly_chart,
    plot_distance_over_time,
    plot_pace_hist,
    plot_indoor_outdoor_donut,
    plot_cumulative_distance,
    plot_elevation_vs_distance,
    plot_weekly_volume,
    plot_gear_usage,
    plot_intensity_distribution,
    plot_time_of_day_analysis,
    plot_heart_rate_zones,
    plot_cadence_analysis,
    plot_temperature_effect,
    plot_activity_calendar,
    plot_pace_vs_heartrate,
    plot_weekly_trends,
    plot_gear_mileage,
    plot_seasonal_trends,
    plot_progress_over_time,
    plot_activity_duration_analysis,
    plot_speed_trends_over_time,
    plot_activity_frequency_heatmap,
    plot_distance_vs_duration,
    plot_monthly_comparison,
    plot_activity_types_over_time,
    plot_performance_metrics_correlation,
    plot_elevation_profile,
    plot_rest_days_analysis,
    plot_sunburst_activity_hierarchy,
    plot_training_load_over_time,
    plot_activity_start_times_radar,
    plot_achievement_milestones,
)

from src.analysis.constants import CURRENT_YEAR
from src.db.db_manager import DatabaseManager
from app.utils import load_data, format_kph_to_pace
from app.app_config import Config


st.set_page_config(page_title="Strava Dashboard", layout="wide")


st.title("🏃‍♂️ Strava Dashboard")

# Load data
df = load_data()

# Convert date and extract month/year properly

logger.info(df["month"].head())

sport_types = ["Walk", "Run", "Ride"]
df = df[df["sport_type"].isin(sport_types)]

""" FILTERING """
# Sidebar filters
st.sidebar.header("Filters")

# default_year = CURRENT_YEAR
years = sorted(df["year"].unique(), reverse=True)
year = st.sidebar.multiselect("Select year", years, default=[Config.DEFAULT_YEAR])

# default_sport = "Run"
sports = df["sport_type"].unique()
# default_index = list(sports).index(default_sport) if default_sport in sports else 0

default_index = list(sports).index(Config.DEFAULT_SPORT_TYPE) if Config.DEFAULT_SPORT_TYPE in sports else 0

sport = st.sidebar.radio(
    "Sport Type",
    options=df["sport_type"].unique(),
    index=default_index  
)

# Get all available months for the selected year(s)
available_months_df = df[df["year"].isin(year)]
month_options = sorted(
    available_months_df["month"].unique(), 
    key=lambda x: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"].index(x)
)

month = st.sidebar.multiselect(
    "Month",
    month_options,
    default=[]  # Empty by default to show all months
)

logger.info(f"Selected month: {month}")

# --- Apply filters ---
if not year:  # If no year selected, show all years
    year_filter = df["year"].unique()
else:
    year_filter = year

if month:
    filtered = df[
        df["year"].isin(year_filter) & 
        (df["sport_type"] == sport) & 
        df["month"].isin(month)
    ].copy()
else:
    filtered = df[
        df["year"].isin(year_filter) & 
        (df["sport_type"] == sport)
    ].copy()

logger.debug(f"Filtered data shape: {filtered.shape}")

# --- Display summary metrics ---
col1, col2, col3, col4, col5 = st.columns(5)

if not filtered.empty:
    col1.metric("Total Distance", f"{filtered['distance'].sum():.1f} km")
    col2.metric("Total Time", f"{filtered['duration'].sum()/60:.1f} hrs")
    col3.metric("Average Speed", f"{filtered['average_speed'].mean():.2f} km/h")
    col4.metric("Average Pace", f"{format_kph_to_pace(filtered['average_speed'].mean())} min/km")
    col4.metric("Average Distance", f"{filtered['distance'].mean():.1f} km")
else:
    col1.metric("Total Distance", "0 km")
    col2.metric("Total Time", "0 hrs")
    col3.metric("Average Speed", "0 km/h")
    col4.metric("Average Pace", "N/A")
    col5.metric("Average Distance", "0 km")

# --- PLOTS SECTION ---
if not filtered.empty:

    fig1 = plot_distance_over_time(filtered)
    fig2 = plot_pace_hist(filtered)
    fig4 = plot_cumulative_distance(filtered)
    fig5 = plot_elevation_vs_distance(filtered)
    fig6 = plot_weekly_volume(filtered)
    fig7 = plot_gear_usage(filtered)
    fig8 = plot_intensity_distribution(filtered)
    fig9 = plot_indoor_outdoor_donut(filtered)
    

    fig10 = plot_time_of_day_analysis(filtered)
    fig11 = plot_heart_rate_zones(filtered)
    fig12 = plot_cadence_analysis(filtered)
    fig13 = plot_temperature_effect(filtered)
    fig14 = plot_activity_calendar(filtered)
    fig15 = plot_pace_vs_heartrate(filtered)
    fig16 = plot_weekly_trends(filtered)
    fig17 = plot_gear_mileage(filtered)
    fig18 = plot_seasonal_trends(filtered)
    fig19 = plot_progress_over_time(filtered)
    

    fig20 = plot_speed_trends_over_time(filtered)
    fig21 = plot_activity_duration_analysis(filtered)
    fig22 = plot_distance_vs_duration(filtered)
    fig23 = plot_activity_frequency_heatmap(filtered)
    fig24 = plot_monthly_comparison(filtered)
    fig25 = plot_activity_types_over_time(filtered)
    fig26 = plot_performance_metrics_correlation(filtered)
    fig27 = plot_elevation_profile(filtered)
    fig28 = plot_rest_days_analysis(filtered)
    fig29 = plot_sunburst_activity_hierarchy(filtered)
    fig30 = plot_training_load_over_time(filtered)
    fig31 = plot_activity_start_times_radar(filtered)
    fig32 = plot_achievement_milestones(filtered)
    
    # Monthly distance summary
    monthly = (
        filtered.groupby(["year", "month"], sort=False)["distance"]
                .sum()
                .reset_index()
    )
    
    # Sort months correctly
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    monthly["month"] = pd.Categorical(monthly["month"], categories=month_order, ordered=True)
    monthly = monthly.sort_values(["year", "month"])
    monthly["distance_label"] = monthly["distance"].round(0)

    fig3 = px.bar(
        monthly,
        x="month",
        y="distance",
        barmode="group",
        title="Total Distance per Month",
        text="distance_label", 
        color_discrete_sequence=["#1f77b4"],
        template="plotly_white"
    )
    fig3.update_traces(
        showlegend=False,
        textposition="outside",
        textfont=dict(size=16, color="white"),
        cliponaxis=False
    )
    
    # Create tabs for different categories
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Overview", "Performance", "Patterns", "Advanced", "Progress", "All Plots"
    ])
    
    with tab1:  # Overview
        col1, col2 = st.columns(2)
        with col1:
            safe_plotly_chart(fig1, "tab1_fig1", "Distance over time")
            safe_plotly_chart(fig9, "tab1_fig9", "Activity type")
            safe_plotly_chart(fig14, "tab1_fig14", "Activity calendar")
        with col2:
            safe_plotly_chart(fig6, "tab1_fig6", "Weekly volume")
            safe_plotly_chart(fig10, "tab1_fig10", "Time of day")
            safe_plotly_chart(fig23, "tab1_fig23", "Activity frequency")
    
    with tab2:  # Performance
        col1, col2 = st.columns(2)
        with col1:
            safe_plotly_chart(fig2, "tab2_fig2", "Pace distribution")
            safe_plotly_chart(fig11, "tab2_fig11", "Heart rate zones")
            safe_plotly_chart(fig12, "tab2_fig12", "Cadence analysis")
            safe_plotly_chart(fig20, "tab2_fig20", "Speed trends")
        with col2:
            safe_plotly_chart(fig5, "tab2_fig5", "Elevation vs distance")
            safe_plotly_chart(fig15, "tab2_fig15", "Pace vs heart rate")
            safe_plotly_chart(fig13, "tab2_fig13", "Temperature effect")
            safe_plotly_chart(fig27, "tab2_fig27", "Elevation profile")
    
    with tab3:  # Patterns
        col1, col2 = st.columns(2)
        with col1:
            safe_plotly_chart(fig16, "tab3_fig16", "Weekly trends")
            safe_plotly_chart(fig18, "tab3_fig18", "Seasonal trends")
            safe_plotly_chart(fig24, "tab3_fig24", "Monthly comparison")
        with col2:
            safe_plotly_chart(fig25, "tab3_fig25", "Activity types over time")
            safe_plotly_chart(fig28, "tab3_fig28", "Rest days analysis")
            safe_plotly_chart(fig31, "tab3_fig31", "Start times radar")
    
    with tab4:  # Advanced
        col1, col2 = st.columns(2)
        with col1:
            safe_plotly_chart(fig21, "tab4_fig21", "Duration analysis")
            safe_plotly_chart(fig22, "tab4_fig22", "Distance vs duration")
            safe_plotly_chart(fig26, "tab4_fig26", "Metrics correlation")
        with col2:
            safe_plotly_chart(fig29, "tab4_fig29", "Activity hierarchy")
            safe_plotly_chart(fig30, "tab4_fig30", "Training load")
            safe_plotly_chart(fig32, "tab4_fig32", "Milestones")
    
    with tab5:  # Progress
        col1, col2 = st.columns(2)
        with col1:
            safe_plotly_chart(fig4, "tab5_fig4", "Cumulative distance")
            safe_plotly_chart(fig19, "tab5_fig19", "Progress over time")
        with col2:
            safe_plotly_chart(fig7, "tab5_fig7", "Gear usage")
            safe_plotly_chart(fig17, "tab5_fig17", "Gear mileage")
    
    with tab6:  # All Plots

        all_plots = [
            (fig1, "Distance Over Time"), (fig2, "Pace Distribution"), 
            (fig3, "Monthly Distance"), (fig4, "Cumulative Distance"), 
            (fig5, "Elevation vs Distance"), (fig6, "Weekly Volume"), 
            (fig7, "Gear Usage"), (fig8, "Intensity Distribution"), 
            (fig9, "Activity Type"), (fig10, "Time of Day"), 
            (fig11, "Heart Rate Zones"), (fig12, "Cadence Analysis"), 
            (fig13, "Temperature Effect"), (fig14, "Activity Calendar"),
            (fig15, "Pace vs Heart Rate"), (fig16, "Weekly Trends"), 
            (fig17, "Gear Mileage"), (fig18, "Seasonal Trends"), 
            (fig19, "Progress Over Time"), (fig20, "Speed Trends"),
            (fig21, "Duration Analysis"), (fig22, "Distance vs Duration"), 
            (fig23, "Frequency Heatmap"), (fig24, "Monthly Comparison"),
            (fig25, "Activity Types Over Time"), (fig26, "Metrics Correlation"), 
            (fig27, "Elevation Profile"), (fig28, "Rest Days Analysis"),
            (fig29, "Activity Hierarchy"), (fig30, "Training Load"), 
            (fig31, "Start Times Radar"), (fig32, "Milestones")
        ]
        
        cols = st.columns(2)
        for i, (fig, name) in enumerate(all_plots):
            with cols[i % 2]:
                if fig is not None:
                    st.subheader(name)
                    safe_plotly_chart(fig, f"all_{i}", name)
else:
    st.warning("No data available for the selected filters.")

def run_streamlit():
    app_path = os.path.join(os.path.dirname(__file__), "app.py")
    subprocess.run(["streamlit", "run", app_path])