# src/clients/weather_client.py
import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from src.utils import map_weather_codes


class WeatherClient:
    BASE_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast?"

    def __init__(self, df: pd.DataFrame):
        self.df = df
        logger.info("Initializing WeatherClient")

    def extract_row_weather(self, row):
        weather_data = row.get("weather_data")
        if not weather_data or "hourly" not in weather_data:
            return None

        rounded_time_str = f"{row['date']}T{row['rounded_start_time']}:00"
        try:
            rounded_time = datetime.strptime(rounded_time_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError as e:
            logger.error(f"Error parsing time: {e}")
            return None

        hourly_times = [
            datetime.strptime(time, "%Y-%m-%dT%H:%M")
            for time in weather_data["hourly"]["time"]
        ]
        if rounded_time in hourly_times:
            index = hourly_times.index(rounded_time)
            return {
                "temperature": weather_data["hourly"]["temperature_2m"][index],
                "precipitation": weather_data["hourly"]["precipitation"][index],
                "weather_code": weather_data["hourly"]["weather_code"][index],
                "wind_speed_10m": weather_data["hourly"]["wind_speed_10m"][index],
                "rain": weather_data["hourly"]["rain"][index],
                "snowfall": weather_data["hourly"]["snowfall"][index],
            }
        else:
            logger.warning(
                f"Rounded time {rounded_time} not found in weather data for row {row.name}"
            )
            return None

    def extract_weather_info(self):
        # Extract weather info and expand into DataFrame columns
        weather_info = self.df.apply(self.extract_row_weather, axis=1).apply(pd.Series)
        return pd.concat([self.df, weather_info], axis=1)

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
        successful_fetches = 0
        total_rows = len(self.df)

        # Add rounded start time and fetch weather data for each row
        self.df["rounded_start_time"] = self.df["start_time"].apply(
            self.round_time_to_nearest_hour
        )
        self.df["weather_data"] = self.df.apply(
            self.fetch_weather_data, axis=1, base_url=self.BASE_URL
        )
        successful_fetches = self.df["weather_data"].notnull().sum()
        logger.debug(
            f"Successfully fetched weather data for {successful_fetches} out of {total_rows} rows"
        )

        return self.extract_weather_info()
    
