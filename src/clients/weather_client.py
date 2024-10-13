# src/clients/weather_client.py
import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from src.database.db import connect_weather_db, connect_activities_db


class WeatherClient:
    BASE_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast?"

    def __init__(self, df: pd.DataFrame):
        self.df = df
        logger.info("Initializing WeatherClient")

    def get_existing_weather_ids(self):
        """Fetch activity IDs from the weather table that already have weather data."""
        conn = connect_weather_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM weather")
        existing_ids = cursor.fetchall()
        conn.close()

        # Convert the list of tuples to a flat list of activity IDs
        return {row[0] for row in existing_ids}

    def get_existing_activity_ids(self):
        """Fetch activity IDs from the activities table."""
        conn = connect_activities_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM activities")
        activity_ids = cursor.fetchall()
        conn.close()

        # Convert the list of tuples to a flat list of activity IDs
        return {row[0] for row in activity_ids}

    def get_weather_data(self):
        # Get all activities and weather entries
        existing_activity_ids = self.get_existing_activity_ids()
        existing_weather_ids = self.get_existing_weather_ids()

        # Filter out activities that already have weather data
        missing_weather_ids = list(
            set(existing_activity_ids) - set(existing_weather_ids)
        )

        if not missing_weather_ids:
            logger.info("All activities already have weather data.")
            return pd.DataFrame()  # No new data to fetch

        # Filter self.df to include only rows with missing weather data
        df_missing_weather = self.df[self.df["id"].isin(missing_weather_ids)]

        # Fetch weather data for those activities
        df_missing_weather["weather_data"] = df_missing_weather.apply(
            self.fetch_weather_data, axis=1, base_url=self.BASE_URL
        )
        logger.info(f"Fetched weather data for {len(df_missing_weather)} activities.")

        # Return the dataframe with fetched weather data
        return self.extract_weather_info(df_missing_weather)

    def extract_weather_info(self, df):
        """Extracts and expands weather information from the fetched data."""
        weather_info = df.apply(self.extract_row_weather, axis=1).apply(pd.Series)
        return pd.concat([df, weather_info], axis=1)

    def round_time_to_nearest_hour(self, time_str):
        # Convert the time string into a datetime object
        time_obj = datetime.strptime(time_str, "%H:%M")

        # Round to the nearest hour
        if time_obj.minute >= 30:
            # Add timedelta of one hour and set minutes to 00
            time_obj = time_obj + timedelta(hours=1)

        # Return the rounded time as a string with format HH:MM
        return time_obj.strftime("%H:00")

    def fetch_weather_data(self, row, base_url):
        try:
            lat, lng = row["lat_lng"].split(", ")
        except ValueError:
            logger.warning(
                f"Invalid lat_lng format in row {row.name}: {row['lat_lng']}"
            )
            return None

        params = {
            "latitude": lat,
            "longitude": lng,
            "start_date": row["date"],
            "end_date": row["date"],
            "hourly": ",".join(
                [
                    "temperature_2m",
                    "precipitation",
                    "weather_code",
                    "wind_speed_10m",
                    "rain",
                    "snowfall",
                ]
            ),
            "wind_speed_unit": "ms",
        }

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"Failed for row {row.name}: {response.status_code}")
            return None

    def get_weather_data(self):
        # Get all activities and weather entries
        existing_activity_ids = self.get_existing_activity_ids()
        existing_weather_ids = self.get_existing_weather_ids()

        # Filter out activities that already have weather data
        missing_weather_ids = list(
            set(existing_activity_ids) - set(existing_weather_ids)
        )

        if not missing_weather_ids:
            logger.info("All activities already have weather data.")
            return pd.DataFrame()  # No new data to fetch

        # Filter self.df to include only rows with missing weather data
        df_missing_weather = self.df[self.df["id"].isin(missing_weather_ids)]

        # Fetch weather data for those activities
        df_missing_weather["weather_data"] = df_missing_weather.apply(
            self.fetch_weather_data, axis=1, base_url=self.BASE_URL
        )
        logger.info(f"Fetched weather data for {len(df_missing_weather)} activities.")

        # Return the dataframe with fetched weather data
        return self.extract_weather_info(df_missing_weather)
