# src/models/processing.py
import pandas as pd
from loguru import logger
from utils import map_months

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns for clarity.
    """
    return df.rename(columns={
        "moving_time": "duration",
        "total_elevation_gain": "elevation_gain",
        "start_date_local": "date",
        "trainer": "indoor",
        "suffer_score": "intensity",
        "start_latlng": "lat_lng"
    })


def convert_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts units for specific columns.
    """
    unit_conversions = {
        "distance": lambda m: m / 1000,  # meters to kilometers
        "duration": lambda sec: sec / 60,  # seconds to minutes
        "average_speed": lambda mps: mps * 3.6  # m/s to km/h
    }
    
    for col, conversion in unit_conversions.items():
        if col in df:
            df[col] = df[col].apply(conversion)
    return df


def split_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Splits the 'date' column into separate date components and times.
    """
    if "date" not in df or "duration" not in df:
        raise ValueError('Missing required columns: "date" and "duration".')

    df["start_time"] = pd.to_datetime(df["date"])
    df["date"] = df["start_time"].dt.strftime("%Y-%m-%d")
    df["month"] = df["start_time"].dt.strftime("%m")
    df["day_of_week"] = df["start_time"].dt.strftime("%A").str.title()
    df["end_time"] = df["start_time"] + pd.to_timedelta(df["duration"], unit="m")
    
    df["start_time"] = df["start_time"].dt.strftime("%H:%M")
    df["end_time"] = df["end_time"].dt.strftime("%H:%M")
    
    return df


def replace_lat_lng_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts lat_lng column lists to comma-separated strings or "" if empty.
    """
    # Ensure lat_lng is always a string
    df["lat_lng"] = df["lat_lng"].apply(
        lambda x: ', '.join(map(str, x)) if isinstance(x, list) and len(x) > 0 else "0, 0"
    )
    return df


def replace_nan_with_mean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces NaN values in numeric columns with the column mean.
    """
    return df.fillna(df.mean(numeric_only=True))


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes raw activity data.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame.")
    df = df[df["sport_type"].isin(["Ride", "Run"])]

    try:
        processed_df = (
            df.pipe(rename_columns)
              .pipe(convert_units)
              .pipe(split_datetime_columns)
              .pipe(replace_nan_with_mean)
              .pipe(replace_lat_lng_values)
              .pipe(map_months)
        )
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

    return processed_df[[
        "id", "date", "month", "day_of_week", "start_time", "end_time", 
        "sport_type", "indoor", "distance", "duration", "elevation_gain", 
        "gear_id", "average_heartrate", "average_speed", "average_cadence", 
        "average_temp", "average_watts", "intensity", "lat_lng"
    ]]