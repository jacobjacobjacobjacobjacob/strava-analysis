# src/models/processing.py
import pandas as pd
from loguru import logger
import ast
from src.utils import map_months, map_weather_codes, PACE_ZONES_MAPPING


def rename_activity_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns for clarity."""
    return df.rename(
        columns={
            "moving_time": "duration",
            "total_elevation_gain": "elevation_gain",
            "start_date_local": "date",
            "trainer": "indoor",
            "suffer_score": "intensity",
            "start_latlng": "lat_lng",
        }
    )


def rename_weather_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns for clarity."""
    return df.rename(
        columns={
            "temperature": "temp",
            "weather_code": "weather_type",
            "wind_speed_10m": "wind",
            "snowfall": "snow",
        }
    )


def convert_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts units for specific columns.
    """
    unit_conversions = {
        "distance": lambda m: m / 1000,  # meters to kilometers
        "duration": lambda sec: sec / 60,  # seconds to minutes
        "average_speed": lambda mps: mps * 3.6,  # m/s to km/h
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
        lambda x: (
            ", ".join(map(str, x)) if isinstance(x, list) and len(x) > 0 else "0, 0"
        )
    )
    return df

def extract_splits_data(df: pd.DataFrame) -> pd.DataFrame:
    """Extracts splits data from the dataframe with detailed activity data"""
    filtered_df = df[["id", "sport_type", "splits_metric", "laps", "available_zones"]]
    return filtered_df


def explode_column(detailed_df_raw, column):
    """Explodes a column of dictionaries into separate columns."""

    # Extract the column
    metric_raw = detailed_df_raw[column]
    # Ensure all rows are valid lists (convert strings to actual Python lists if necessary)
    metric_cleaned = metric_raw.apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )
    # Drop rows with NaN or invalid values
    metric_cleaned = metric_cleaned.dropna()
    
    # Explode the list of dictionaries into individual rows
    metric_exploded = metric_cleaned.explode()
   
    # Normalize the dictionary entries into columns
    metric_df = pd.json_normalize(metric_exploded)

    return metric_df

def clean_laps_data(df: pd.DataFrame) -> pd.DataFrame:
    columns_to_drop_laps = ["id", "start_date_local", "resource_state", "start_date", "max_speed", "start_index", "end_index", "device_watts", "max_heartrate", "activity.visibility", "activity.resource_state", "athlete.id", "elapsed_time", "athlete.resource_state"]
    df = df.drop(columns=columns_to_drop_laps, axis=1)
    
    # Rename columns
    laps_rename_mapping = {
    "name": "lap_name",
    "moving_time": "duration",
    "total_elevation_gain": "elevation_gain",
    "activity.id": "id",
}
    df = df.rename(columns=laps_rename_mapping)
    
    # Map pace zones
    df["pace_zone"] = df["pace_zone"].map(PACE_ZONES_MAPPING)
    
    
    # Convert duration from seconds to minutes
    df["duration"] = df["duration"] / 60
    
    # Convert speeds from m/s to km/h
    df["average_speed"] = df["average_speed"] * 3.6


    
    # Round specific columns
    laps_columns_to_round = ["duration", "average_speed", "distance", "average_heartrate"]
    df[laps_columns_to_round] = df[laps_columns_to_round].round(1)
    
    return df

def clean_splits_data(df: pd.DataFrame) -> pd.DataFrame:
    # Rename columns
    splits_rename_mapping = {
    "moving_time": "duration",
    "average_grade_adjusted_speed": "average_gap"
    }
    df = df.rename(columns=splits_rename_mapping)
    
    # Map pace zones
    df["pace_zone"] = df["pace_zone"].map(PACE_ZONES)
    
    # Drop unnecessary columns
    df = df.drop(columns="elapsed_time")
    
    # Convert duration from seconds to minutes
    df["duration"] = df["duration"] / 60
    
    # Convert speeds from m/s to km/h
    speed_columns = ["average_speed", "average_gap"]
    df[speed_columns] = df[speed_columns] * 3.6
    
    # Round specific columns
    columns_to_round = ["duration", "average_speed", "average_gap", "average_heartrate"]
    df[columns_to_round] = df[columns_to_round].round(1)
    
    return df




def process_activity_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes raw activity data.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame.")
    df = df[df["sport_type"].isin(["Ride", "Run"])]

    try:
        processed_df = (
            df.pipe(rename_activity_columns)
            .pipe(convert_units)
            .pipe(split_datetime_columns)
            .pipe(replace_lat_lng_values)
            .pipe(map_months)
        )
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

    return processed_df[
        [
            "id",
            "date",
            "month",
            "day_of_week",
            "start_time",
            "end_time",
            "sport_type",
            "indoor",
            "distance",
            "duration",
            "elevation_gain",
            "gear_id",
            "average_heartrate",
            "average_speed",
            "average_cadence",
            "average_temp",
            "average_watts",
            "intensity",
            "lat_lng",
        ]
    ]


def process_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes weather data.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame.")

    try:
        # Check if the weather_code column exists
        if "weather_code" in df.columns:
            df = df.pipe(map_weather_codes)

        processed_df = df.pipe(rename_weather_columns)
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

    return processed_df[
        ["id", "date", "temp", "weather_type", "precipitation", "rain", "wind", "snow"]
    ]
