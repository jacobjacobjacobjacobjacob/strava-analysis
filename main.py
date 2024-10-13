# main.py
import os
import sys
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from src.clients.strava_client import StravaClient
from src.clients.weather_client import WeatherClient
from src.models.processing import process_activity_data, process_weather_data
from src.models.activity import Activity
from src.models.weather import Weather
from src.models.gear import Gear
from src.database.db import (
    create_activities_table,
    create_gear_table,
    create_weather_table,
)


def main():
    load_dotenv()
    strava_client = StravaClient(
        client_id=os.getenv("STRAVA_CLIENT_ID"),
        client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
        refresh_token=os.getenv("STRAVA_REFRESH_TOKEN"),
        athlete_id=os.getenv("STRAVA_ATHLETE_ID"),
    )

    # Create tables
    create_activities_table()
    create_weather_table()
    create_gear_table()

    # Fetch activities
    activities = strava_client.get_activities()
    activities_df = pd.DataFrame(activities)
    activities_df = process_activity_data(activities_df)

    # Fetch weather for activities that don't have it
    weather_client = WeatherClient(activities_df.copy())
    weather_df = weather_client.get_weather_data()
    # weather_df = process_weather_data(weather_df)

    # Only process weather data if there are new rows
    if not weather_df.empty:
        weather_df = process_weather_data(weather_df)
        Weather.process_weather(weather_df)

    # Process and save activities to the database
    Activity.process_activities(activities_df)

    # Get unique gear IDs and process them
    gear_list = activities_df["gear_id"].dropna().unique().tolist()
    Gear.process_gears(strava_client, gear_list)

    return activities_df, weather_df


if __name__ == "__main__":
    activities_df, weather_df = main()

    # activity = client.get_detailed_activity(activity_id="12455871622")
    # zones = client.get_activity_zones(activity_id="12455871622")
