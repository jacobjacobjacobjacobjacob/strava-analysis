# src/models/processing.py
import pandas as pd
from loguru import logger

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns of the DataFrame for clarity.

    :param df: Input DataFrame with raw column names.
    :return: DataFrame with renamed columns.
    """
    rename_mapping = {
        "moving_time": "duration",                   # Moving time (in seconds) -> duration (in minutes)
        "total_elevation_gain": "elevation_gain",    # Elevation gain (in meters)
        "start_date_local": "date",                  # Local start date (ISO format)
        "trainer": "indoor",                         # Boolean for indoor training
        "suffer_score": "intensity"                  # Strava suffer score -> intensity
    }
    return df.rename(columns=rename_mapping)


def convert_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts units of specific columns for readability.
    - Converts distance from meters to kilometers.
    - Converts duration from seconds to minutes.
    - Converts speed from meters per second to kilometers per hour.

    :param df: DataFrame with raw units.
    :return: DataFrame with converted units.
    """
    # Unit conversion functions
    m_to_km = lambda m: m / 1000
    sec_to_min = lambda sec: sec / 60
    mps_to_kph = lambda mps: mps * 3.6

    # Mapping columns to their respective conversion functions
    conversion_mapping = {
        "distance": m_to_km,
        "duration": sec_to_min,
        "average_speed": mps_to_kph,
    }

    # Apply conversion to relevant columns
    for column, conversion_func in conversion_mapping.items():
        if column in df.columns:
            df[column] = df[column].apply(conversion_func)

    return df


def split_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Splits the "date" column into separate "date", "month", "day_of_week", "start_time", and "end_time" columns.
    The "start_time" is extracted from the "date", and the "end_time" is calculated by adding the duration to the "start_time".
    
    :param df: DataFrame containing "date" and "duration" columns.
    :return: DataFrame with additional date-related columns.
    :raises ValueError: If required columns are missing.
    """
   # Check that required columns exist
    if "date" not in df.columns or "duration" not in df.columns:
        raise ValueError('The DataFrame must contain "date" and "duration" columns.')

    # Full datetime conversion of 'date' column
    df["start_time"] = pd.to_datetime(df["date"])

    # Extract the 'date' in YYYY-MM-DD format
    df["date"] = df["start_time"].dt.strftime("%Y-%m-%d")

    # Extract the 'month' (MM format)
    df["month"] = df["start_time"].dt.strftime("%m")

    # Extract the 'day_of_week''
    df["day_of_week"] = df["start_time"].dt.strftime("%A").str.title()

    # Calculate end_time by adding the duration in minutes
    df["end_time"] = df["start_time"] + pd.to_timedelta(df["duration"], unit="m")

    # Format 'start_time' and 'end_time' as HH:MM
    df["start_time"] = df["start_time"].dt.strftime("%H:%M")
    df["end_time"] = df["end_time"].dt.strftime("%H:%M")

    return df


def replace_nan_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces NaN values in specific columns with the column's mean value.
    This helps handle missing data in metrics like heart rate and intensity.

    :param df: DataFrame that may contain NaN values in specific columns.
    :return: DataFrame with NaN values replaced by the mean of their respective columns.
    """
    columns_to_fill = ["average_heartrate", "intensity"]

    for column in columns_to_fill:
        if column in df.columns:
            mean_value = df[column].mean()  # Calculate column mean
            df[column] = df[column].fillna(mean_value)  # Replace NaN with the mean

    return df


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the raw data into a cleaned and converted format by:
    1. Renaming columns for readability.
    2. Converting units to more standard ones (e.g., km, minutes, kph).
    3. Splitting datetime columns into separate date, start time, and end time.
    4. Replacing missing values with the mean for specific columns.
    
    :param df: Raw DataFrame containing activity data.
    :return: Cleaned and processed DataFrame.
    :raises Exception: If any error occurs during processing.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a Pandas DataFrame.")


    # Filter for only "Ride" and "Run" activities
    df = df[df["sport_type"].isin(["Ride", "Run"])]



    try:
        processed_df = (
            df.pipe(rename_columns)
              .pipe(convert_units)
              .pipe(split_datetime_columns)
              .pipe(replace_nan_values)
        )
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

    columns = [
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
        "intensity"
    ]
    processed_df = processed_df[columns]


    return processed_df

