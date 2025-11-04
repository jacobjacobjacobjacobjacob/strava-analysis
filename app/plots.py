# app/plots.py
import plotly.express as px
import pandas as pd
import streamlit as st

def plot_distance_over_time(df):
    return px.bar(df, x="date", y="distance", title="Distance Over Time")

def plot_pace_hist(df):
    print("\n\n\n\n\n\n\nNOTE TO SELF:\n Separate km/h to Pace (min/km) for better user understanding.")
    return px.histogram(df, x="average_speed", nbins=30, title="Pace Distribution")

def plot_indoor_outdoor_donut(df):
    """Donut chart showing indoor vs outdoor activities"""
    if 'indoor' not in df.columns:
        return px.pie(title="No indoor/outdoor data available")
    
    indoor_counts = df['indoor'].value_counts().reset_index()
    indoor_counts.columns = ['type', 'count']
    indoor_counts['type'] = indoor_counts['type'].map({0: 'Outdoor', 1: 'Indoor'})
    
    fig = px.pie(
        indoor_counts, 
        values='count', 
        names='type',
        title="Indoor vs Outdoor Activities",
        hole=0.4,
        color_discrete_sequence=['#00CC96', '#FF6692']
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

def plot_cumulative_distance(df):
    """Area chart showing cumulative distance over time"""
    df_sorted = df.sort_values('date').copy()
    df_sorted['cumulative_distance'] = df_sorted['distance'].cumsum()
    
    fig = px.area(
        df_sorted,
        x='date',
        y='cumulative_distance',
        title="Cumulative Distance Over Time",
        color_discrete_sequence=['#636EFA']
    )
    fig.update_layout(yaxis_title="Cumulative Distance (km)")
    return fig

def plot_elevation_vs_distance(df):
    """Scatter plot of elevation gain vs distance"""
    fig = px.scatter(
        df,
        x='distance',
        y='elevation_gain',
        color='sport_type',
        title="Elevation Gain vs Distance",
        hover_data=['name', 'date'],
        size='distance',  # Size points by distance
        size_max=15
    )
    fig.update_layout(
        xaxis_title="Distance (km)",
        yaxis_title="Elevation Gain (m)"
    )
    return fig

def plot_weekly_volume(df):
    """Bar chart showing activity volume by day of week"""
    weekly = df.groupby('day_of_week', as_index=False).agg({
        'distance': 'sum',
        'id': 'count'  # Count of activities
    })
    weekly.columns = ['day_of_week', 'total_distance', 'activity_count']
    
    # Order days properly
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekly['day_of_week'] = pd.Categorical(weekly['day_of_week'], categories=day_order, ordered=True)
    weekly = weekly.sort_values('day_of_week')
    
    fig = px.bar(
        weekly,
        x='day_of_week',
        y='total_distance',
        title="Weekly Activity Volume",
        text='activity_count',
        color_discrete_sequence=['#EF553B']
    )
    fig.update_traces(
        texttemplate='%{text} activities',
        textposition='outside'
    )
    fig.update_layout(yaxis_title="Total Distance (km)")
    return fig

def plot_gear_usage(df):
    """Bar chart showing distance by gear (bikes)"""
    if 'gear_id' not in df.columns or df['gear_id'].isna().all():
        return px.bar(title="No gear data available")
    
    gear_usage = df[df['gear_id'].notna()].groupby('gear_id').agg({
        'distance': 'sum',
        'id': 'count'
    }).reset_index()
    
    gear_usage.columns = ['gear_id', 'total_distance', 'activity_count']
    gear_usage = gear_usage.sort_values('total_distance', ascending=True)
    
    fig = px.bar(
        gear_usage,
        y='gear_id',
        x='total_distance',
        title="Distance by Gear",
        orientation='h',
        text='activity_count',
        color_discrete_sequence=['#AB63FA']
    )
    fig.update_traces(
        texttemplate='%{text} rides',
        textposition='outside'
    )
    fig.update_layout(
        xaxis_title="Total Distance (km)",
        yaxis_title="Gear ID"
    )
    print("NOTE TO SELF:\n Map gear ID to Gear name, and separate Bikes from Shoes. \n Total distance by SHOES, not by GEAR. Total RIDES vs Total RUNS.\n\n\n\n\n\n")
    return fig

def plot_intensity_distribution(df):
    """Histogram of workout intensity"""
    if 'intensity' not in df.columns:
        return px.histogram(title="No intensity data available")
    
    fig = px.histogram(
        df,
        x='intensity',
        nbins=20,
        title="Workout Intensity Distribution",
        color_discrete_sequence=['#FFA15A']
    )
    fig.update_layout(
        xaxis_title="Intensity",
        yaxis_title="Number of Activities"
    )
    return fig



# Add these new plot functions to your plots.py file

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

def plot_time_of_day_analysis(df):
    """Analyze what time of day activities typically occur"""
    if 'start_time' not in df.columns:
        return create_empty_plot("No time data available")
    
    df_copy = df.copy()
    df_copy['hour'] = pd.to_datetime(df_copy['start_time']).dt.hour
    time_counts = df_copy['hour'].value_counts().sort_index()
    
    fig = px.bar(x=time_counts.index, y=time_counts.values,
                 title="Activity Distribution by Time of Day",
                 labels={'x': 'Hour of Day', 'y': 'Number of Activities'})
    fig.update_layout(xaxis=dict(tickmode='linear', dtick=1))
    return fig

def plot_heart_rate_zones(df):
    """Plot heart rate zone distribution if data available"""
    if 'average_heartrate' not in df.columns or df['average_heartrate'].isna().all():
        return create_empty_plot("No heart rate data available")
    
    # Simple HR zones (you can customize these based on max HR)
    hr_zones = {
        'Zone 1 (Easy)': (100, 147),
        'Zone 2 (Aerobic)': (148, 167),
        'Zone 3 (Tempo)': (168, 178),
        'Zone 4 (Threshold)': (179, 190),
        'Zone 5 (Max)': (191, 220)
    }
    
    zone_counts = {}
    for zone, (low, high) in hr_zones.items():
        zone_counts[zone] = len(df[(df['average_heartrate'] >= low) & 
                                 (df['average_heartrate'] <= high)])
    
    fig = px.pie(values=list(zone_counts.values()), names=list(zone_counts.keys()),
                 title="Heart Rate Zone Distribution")
    return fig

def plot_cadence_analysis(df):
    """Analyze running cadence if available"""
    if 'average_cadence' not in df.columns or df['average_cadence'].isna().all():
        return create_empty_plot("No cadence data available")
    
    fig = px.histogram(df, x='average_cadence', nbins=20,
                       title="Cadence Distribution",
                       labels={'average_cadence': 'Cadence (steps/min)'})
    return fig

# Updated plots.py with fixes for JSON serialization issues
def plot_temperature_effect(df):
    """Compare performance in different temperature conditions"""
    temp_column = 'average_temp'
    if temp_column not in df.columns or df[temp_column].isna().all():
        return create_empty_plot("No temperature data available")
    
    try:
        df_copy = df.copy()
        df_copy = df_copy.dropna(subset=[temp_column])
        
        # Instead of using pd.cut which creates Interval objects, create string labels
        temp_bins = [df_copy[temp_column].min(), 10, 15, 20, 25, df_copy[temp_column].max()]
        temp_labels = [f"{temp_bins[i]:.0f}°-{temp_bins[i+1]:.0f}°C" for i in range(len(temp_bins)-1)]
        
        df_copy['temp_range'] = pd.cut(df_copy[temp_column], bins=temp_bins, labels=temp_labels, include_lowest=True)
        
        fig = px.box(df_copy, x='temp_range', y='average_speed',
                     title="Speed vs Temperature",
                     labels={'average_speed': 'Average Speed (km/h)', 
                            'temp_range': 'Temperature Range'})
        fig.update_xaxes(tickangle=45)
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting temperature effect: {e}")

def plot_activity_calendar(df):
    """Create a calendar heatmap of activities"""
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['date'])
    df_copy['weekday'] = df_copy['date'].dt.day_name()
    df_copy['week'] = df_copy['date'].dt.isocalendar().week
    df_copy['year'] = df_copy['date'].dt.year
    
    # Create pivot table for heatmap
    calendar_data = df_copy.groupby(['year', 'week', 'weekday']).size().reset_index(name='count')
    
    # Order weekdays properly
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    calendar_data['weekday'] = pd.Categorical(calendar_data['weekday'], categories=weekday_order, ordered=True)
    
    fig = px.density_heatmap(calendar_data, x='weekday', y='week', z='count',
                             title="Activity Calendar Heatmap",
                             color_continuous_scale="Viridis")
    return fig

def plot_pace_vs_heartrate(df):
    """Scatter plot of pace vs heart rate"""
    if any(col not in df.columns for col in ['average_speed', 'average_heartrate']):
        return create_empty_plot("Pace/HR data not available")
    
    df_copy = df.dropna(subset=['average_speed', 'average_heartrate'])
    
    fig = px.scatter(df_copy, x='average_speed', y='average_heartrate',
                     title="Pace vs Heart Rate",
                     labels={'average_speed': 'Speed (km/h)', 
                            'average_heartrate': 'Average Heart Rate'},
                     trendline="lowess")
    return fig

def plot_weekly_trends(df):
    """Show how metrics change throughout the week"""
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['date'])
    df_copy['weekday'] = df_copy['date'].dt.day_name()
    df_copy['weekday_num'] = df_copy['date'].dt.dayofweek
    
    # Order by day of week
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df_copy['weekday'] = pd.Categorical(df_copy['weekday'], categories=weekday_order, ordered=True)
    
    weekly_avg = df_copy.groupby('weekday').agg({
        'distance': 'mean',
        'average_speed': 'mean',
        'elevation_gain': 'mean'
    }).reset_index()
    
    fig = make_subplots(rows=3, cols=1, 
                       subplot_titles=('Average Distance', 'Average Speed', 'Average Elevation Gain'))
    
    fig.add_trace(go.Bar(x=weekly_avg['weekday'], y=weekly_avg['distance']), row=1, col=1)
    fig.add_trace(go.Bar(x=weekly_avg['weekday'], y=weekly_avg['average_speed']), row=2, col=1)
    fig.add_trace(go.Bar(x=weekly_avg['weekday'], y=weekly_avg['elevation_gain']), row=3, col=1)
    
    fig.update_layout(height=600, title_text="Weekly Activity Patterns")
    return fig

def plot_gear_mileage(df):
    """Show cumulative mileage for each gear item"""
    if 'gear_id' not in df.columns or df['gear_id'].isna().all():
        return create_empty_plot("No gear data available")
    
    gear_mileage = df.groupby('gear_id').agg({
        'distance': 'sum',
        'date': 'count'
    }).reset_index()
    gear_mileage.columns = ['Gear', 'Total Distance (km)', 'Activity Count']
    
    fig = px.bar(gear_mileage, x='Gear', y='Total Distance (km)',
                 title="Total Distance by Gear",
                 hover_data=['Activity Count'])
    return fig

def plot_seasonal_trends(df):
    """Analyze how performance changes by season"""
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['date'])
    df_copy['month'] = df_copy['date'].dt.month
    
    # Define seasons
    season_map = {
        12: 'Winter', 1: 'Winter', 2: 'Winter',
        3: 'Spring', 4: 'Spring', 5: 'Spring',
        6: 'Summer', 7: 'Summer', 8: 'Summer', 
        9: 'Fall', 10: 'Fall', 11: 'Fall'
    }
    df_copy['season'] = df_copy['month'].map(season_map)
    
    seasonal_avg = df_copy.groupby('season').agg({
        'distance': 'mean',
        'average_speed': 'mean',
        'elevation_gain': 'mean'
    }).reset_index()
    
    # Order seasons properly
    season_order = ['Winter', 'Spring', 'Summer', 'Fall']
    seasonal_avg['season'] = pd.Categorical(seasonal_avg['season'], categories=season_order, ordered=True)
    seasonal_avg = seasonal_avg.sort_values('season')
    
    fig = make_subplots(rows=1, cols=3, 
                       subplot_titles=('Avg Distance', 'Avg Speed', 'Avg Elevation'))
    
    fig.add_trace(go.Bar(x=seasonal_avg['season'], y=seasonal_avg['distance']), row=1, col=1)
    fig.add_trace(go.Bar(x=seasonal_avg['season'], y=seasonal_avg['average_speed']), row=1, col=2)
    fig.add_trace(go.Bar(x=seasonal_avg['season'], y=seasonal_avg['elevation_gain']), row=1, col=3)
    
    fig.update_layout(height=400, title_text="Seasonal Performance Trends")
    return fig

def plot_progress_over_time(df):
    """Show progress in key metrics over time"""
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['date'])
    df_copy = df_copy.sort_values('date')
    
    # Calculate rolling averages
    df_copy['rolling_distance'] = df_copy['distance'].rolling(window=20, min_periods=1).mean()
    df_copy['rolling_speed'] = df_copy['average_speed'].rolling(window=20, min_periods=1).mean()
    
    fig = make_subplots(rows=2, cols=1, 
                       subplot_titles=('Rolling Average Distance (20 activities)', 
                                      'Rolling Average Speed (20 activities)'))
    
    fig.add_trace(go.Scatter(x=df_copy['date'], y=df_copy['rolling_distance'], 
                            mode='lines', name='Distance'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_copy['date'], y=df_copy['rolling_speed'], 
                            mode='lines', name='Speed'), row=2, col=1)
    
    fig.update_layout(height=500, title_text="Progress Over Time (Rolling Averages)")
    return fig

def create_empty_plot(message):
    """Create an empty plot with a message"""
    fig = go.Figure()
    fig.add_annotation(text=message, xref="paper", yref="paper",
                      x=0.5, y=0.5, showarrow=False,
                      font=dict(size=16))
    return fig




# Add these new plot functions to your plots.py

def plot_activity_duration_analysis(df):
    """Analyze activity duration patterns"""
    try:
        df_copy = df.copy()
        # Convert seconds to hours for better readability
        df_copy['duration_hours'] = df_copy['duration'] / 3600
        
        fig = px.histogram(df_copy, x='duration_hours', nbins=20,
                          title="Activity Duration Distribution",
                          labels={'duration_hours': 'Duration (hours)'})
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting duration analysis: {e}")

def plot_speed_trends_over_time(df):
    """Show how speed changes over time"""
    try:
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy = df_copy.sort_values('date')
        
        # Calculate rolling average of speed
        df_copy['rolling_speed'] = df_copy['average_speed'].rolling(window=7, min_periods=1).mean()
        
        fig = px.scatter(df_copy, x='date', y='average_speed',
                         title="Speed Trends Over Time",
                         labels={'average_speed': 'Speed (km/h)', 'date': 'Date'},
                         trendline="lowess")
        fig.add_scatter(x=df_copy['date'], y=df_copy['rolling_speed'], 
                       mode='lines', name='7-Activity Rolling Avg', line=dict(width=3))
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting speed trends: {e}")

def plot_activity_frequency_heatmap(df):
    """Create a detailed heatmap of activity frequency by day of week and month"""
    try:
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy['day_of_week'] = df_copy['date'].dt.day_name()
        df_copy['month'] = df_copy['date'].dt.month_name()
        df_copy['year'] = df_copy['date'].dt.year
        
        # Create pivot table
        heatmap_data = df_copy.groupby(['day_of_week', 'month']).size().reset_index(name='count')
        
        # Order days and months properly
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                      'July', 'August', 'September', 'October', 'November', 'December']
        
        heatmap_data['day_of_week'] = pd.Categorical(heatmap_data['day_of_week'], categories=day_order, ordered=True)
        heatmap_data['month'] = pd.Categorical(heatmap_data['month'], categories=month_order, ordered=True)
        
        fig = px.density_heatmap(heatmap_data, x='month', y='day_of_week', z='count',
                                title="Activity Frequency Heatmap",
                                color_continuous_scale="Blues")
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting frequency heatmap: {e}")

def plot_distance_vs_duration(df):
    """Scatter plot of distance vs duration with pace lines"""
    try:
        df_copy = df.copy()
        df_copy['duration_hours'] = df_copy['duration'] / 3600
        
        fig = px.scatter(df_copy, x='duration_hours', y='distance',
                        title="Distance vs Duration",
                        labels={'duration_hours': 'Duration (hours)', 'distance': 'Distance (km)'},
                        trendline="lowess")
        
        # Add pace reference lines (km/h)
        for pace in [5, 10, 15, 20]:
            x_line = np.linspace(0.1, df_copy['duration_hours'].max(), 10)
            y_line = pace * x_line
            fig.add_trace(go.Scatter(x=x_line, y=y_line, mode='lines',
                                   line=dict(dash='dash', color='gray'),
                                   name=f'{pace} km/h', showlegend=False))
        
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting distance vs duration: {e}")

def plot_monthly_comparison(df):
    """Compare monthly performance across years"""
    try:
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy['month'] = df_copy['date'].dt.month_name()
        df_copy['year'] = df_copy['date'].dt.year
        
        monthly_stats = df_copy.groupby(['year', 'month']).agg({
            'distance': 'sum',
            'duration': 'sum',
            'average_speed': 'mean'
        }).reset_index()
        
        monthly_stats['duration_hours'] = monthly_stats['duration'] / 3600
        
        # Order months properly
        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                      'July', 'August', 'September', 'October', 'November', 'December']
        monthly_stats['month'] = pd.Categorical(monthly_stats['month'], categories=month_order, ordered=True)
        monthly_stats = monthly_stats.sort_values(['year', 'month'])
        
        fig = px.line(monthly_stats, x='month', y='distance', color='year',
                     title="Monthly Distance Comparison by Year",
                     labels={'distance': 'Total Distance (km)', 'month': 'Month'})
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting monthly comparison: {e}")

def plot_activity_types_over_time(df):
    """Show how activity type distribution changes over time"""
    try:
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy['month_year'] = df_copy['date'].dt.to_period('M').astype(str)
        
        type_over_time = df_copy.groupby(['month_year', 'sport_type']).size().reset_index(name='count')
        
        fig = px.area(type_over_time, x='month_year', y='count', color='sport_type',
                     title="Activity Types Over Time",
                     labels={'count': 'Number of Activities', 'month_year': 'Month'})
        fig.update_xaxes(tickangle=45)
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting activity types over time: {e}")

def plot_performance_metrics_correlation(df):
    """Correlation heatmap between different performance metrics"""
    try:
        # Select numeric columns for correlation
        numeric_cols = ['distance', 'duration', 'average_speed', 'max_speed', 
                       'total_elevation_gain', 'average_heartrate', 'max_heartrate',
                       'average_cadence', 'average_temp']
        
        available_cols = [col for col in numeric_cols if col in df.columns]
        
        if len(available_cols) < 2:
            return create_empty_plot("Not enough numeric metrics for correlation analysis")
        
        corr_matrix = df[available_cols].corr(numeric_only=True)
        
        fig = px.imshow(corr_matrix,
                       title="Performance Metrics Correlation",
                       color_continuous_scale="RdBu_r",
                       aspect="auto")
        fig.update_layout(xaxis_tickangle=-45)
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting correlation: {e}")

def plot_elevation_profile(df):
    """Analyze elevation gain patterns"""
    try:
        elevation_col = None
        for col in ['total_elevation_gain', 'elev_high', 'elev_low']:
            if col in df.columns and not df[col].isna().all():
                elevation_col = col
                break
        
        if not elevation_col:
            return create_empty_plot("No elevation data available")
        
        df_copy = df.copy()
        df_copy = df_copy.dropna(subset=[elevation_col, 'distance'])
        
        # Create elevation gain per km
        df_copy['elevation_per_km'] = df_copy[elevation_col] / df_copy['distance']
        
        fig = px.scatter(df_copy, x='distance', y='elevation_per_km',
                        title="Elevation Gain per Kilometer",
                        labels={'distance': 'Distance (km)', 
                               'elevation_per_km': 'Elevation Gain per km (m/km)'},
                        trendline="lowess")
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting elevation profile: {e}")

def plot_rest_days_analysis(df):
    """Analyze rest patterns between activities"""
    try:
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy = df_copy.sort_values('date')
        
        # Calculate days between activities
        df_copy['days_since_last'] = df_copy['date'].diff().dt.days
        
        # Remove first row (no previous activity)
        rest_days = df_copy['days_since_last'].dropna()
        
        fig = px.histogram(x=rest_days, nbins=20,
                          title="Rest Days Between Activities",
                          labels={'x': 'Days Between Activities', 'y': 'Frequency'})
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting rest days analysis: {e}")

def plot_sunburst_activity_hierarchy(df):
    """Create a sunburst chart of activities by type and time"""
    try:
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy['year'] = df_copy['date'].dt.year
        df_copy['month'] = df_copy['date'].dt.month_name()
        df_copy['season'] = df_copy['date'].dt.month.map({
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring', 4: 'Spring', 5: 'Spring',
            6: 'Summer', 7: 'Summer', 8: 'Summer', 
            9: 'Fall', 10: 'Fall', 11: 'Fall'
        })
        
        # Count activities by hierarchy
        hierarchy_data = df_copy.groupby(['year', 'season', 'month', 'sport_type']).size().reset_index(name='count')
        
        fig = px.sunburst(hierarchy_data, path=['year', 'season', 'month', 'sport_type'], values='count',
                         title="Activity Hierarchy by Time and Type")
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting sunburst: {e}")

def plot_training_load_over_time(df):
    """Estimate training load using distance and duration"""
    try:
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy = df_copy.sort_values('date')
        
        # Simple training load metric (distance * duration)
        df_copy['training_load'] = df_copy['distance'] * (df_copy['duration'] / 3600)
        
        # Calculate rolling training load
        df_copy['rolling_load'] = df_copy['training_load'].rolling(window=7, min_periods=1).sum()
        
        fig = px.line(df_copy, x='date', y=['training_load', 'rolling_load'],
                     title="Training Load Over Time",
                     labels={'value': 'Training Load', 'date': 'Date'})
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting training load: {e}")

def plot_activity_start_times_radar(df):
    """Radar chart showing preferred activity start times"""
    try:
        if 'start_time' not in df.columns:
            return create_empty_plot("No start time data available")
        
        df_copy = df.copy()
        df_copy['hour'] = pd.to_datetime(df_copy['start_time']).dt.hour
        
        # Group hours into time periods
        time_periods = {
            'Early Morning\n(4-7)': (4, 7),
            'Morning\n(8-11)': (8, 11),
            'Afternoon\n(12-16)': (12, 16),
            'Evening\n(17-20)': (17, 20),
            'Night\n(21-3)': (21, 24)
        }
        
        period_counts = {}
        for period, (start, end) in time_periods.items():
            if end > start:
                count = len(df_copy[(df_copy['hour'] >= start) & (df_copy['hour'] < end)])
            else:  # Handle overnight (21-3)
                count = len(df_copy[(df_copy['hour'] >= start) | (df_copy['hour'] < 3)])
            period_counts[period] = count
        
        fig = px.line_polar(r=list(period_counts.values()), theta=list(period_counts.keys()),
                           title="Activity Start Times Distribution",
                           line_close=True)
        fig.update_traces(fill='toself')
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting start times radar: {e}")

def plot_achievement_milestones(df):
    """Show progress towards distance milestones"""
    try:
        df_copy = df.copy()
        df_copy['date'] = pd.to_datetime(df_copy['date'])
        df_copy = df_copy.sort_values('date')
        
        # Calculate cumulative distance
        df_copy['cumulative_distance'] = df_copy['distance'].cumsum()
        
        # Define milestones
        milestones = [50, 100, 250, 500, 750, 1000, 1500, 2000]
        
        milestone_data = []
        for milestone in milestones:
            milestone_date = df_copy[df_copy['cumulative_distance'] >= milestone]
            if not milestone_date.empty:
                milestone_data.append({
                    'milestone': f'{milestone} km',
                    'date': milestone_date.iloc[0]['date'],
                    'activities': len(milestone_date)
                })
        
        if not milestone_data:
            return create_empty_plot("No milestones achieved yet")
        
        milestone_df = pd.DataFrame(milestone_data)
        
        fig = px.scatter(milestone_df, x='date', y='milestone',
                        title="Distance Milestones Achieved",
                        labels={'date': 'Date', 'milestone': 'Distance Milestone'})
        return fig
    except Exception as e:
        return create_empty_plot(f"Error plotting milestones: {e}")
    
def safe_plotly_chart(fig, key, alt_text=""):
    """Safely display a plotly chart with error handling"""
    try:
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True, key=key)
        else:
            st.warning(f"Could not generate plot: {alt_text}")
    except Exception as e:
        st.error(f"Error displaying plot: {str(e)}")
        st.info(f"Plot key: {key}")