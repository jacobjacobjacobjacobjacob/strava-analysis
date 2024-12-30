# utils.py
import time
import pandas as pd
from loguru import logger



def fetch_detailed_activity_with_retries(strava_client, activity_id, retries=3):
    for attempt in range(retries):
        try:
            detailed_activity = strava_client.get_detailed_activity(activity_id)
            logger.debug(f"Fetched detailed activity for ID {activity_id}")
            return detailed_activity
        except Exception as e:
            logger.error(f"Error fetching detailed activity for ID {activity_id}: {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying... ({attempt + 1}/{retries})")
            else:
                logger.error(f"Failed to fetch detailed activity for ID {activity_id} after {retries} attempts")
    return None

def map_months(df: pd.DataFrame):
    df["month"] = df["month"].map(MONTH_MAPPING)
    return df


def map_weather_codes(df: pd.DataFrame):
    df["weather_code"] = df["weather_code"].map(WEATHER_CODE_MAPPING)
    return df


MONTH_MAPPING = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec",
}

MONTH_ORDER = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

WEEKDAY_MAPPING = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}

WEEKDAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

WEATHER_CODE_MAPPING = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    56: "Light freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    66: "Light freezing rain",
    67: "Heavy freezing rain",
    71: "Slight snow fall",
    73: "Moderate snow fall",
    75: "Heavy snow fall",
    77: "Snow grains",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    82: "Violent rain showers",
    85: "Slight snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with slight hail",
    99: "Thunderstorm with heavy hail",
}

PACE_ZONES_MAPPING = {
    1: "Z1 - Active Recovery",
    2: "Z2 - Endurance",
    3: "Z3 - Tempo",
    4: "Z4 - Threshold",
    5: "Z5 - VO2 Max",
    6: "Z6 - Anaerobic"
}

VALID_STREAM_TYPES = [
            "time",
            "distance",
            "latlng",
            "altitude",
            "velocity_smooth",
            "heartrate",
            "cadence",
            "watts",
            "temp",
            "moving",
            "grade_smooth",
        ]