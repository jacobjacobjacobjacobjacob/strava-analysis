import os
import sys
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from src.clients.strava_client import StravaClient
from src.clients.weather_client import WeatherClient
from src.models.processing import process_activity_data, process_weather_data, extract_splits_data
from src.models.activity import Activity
from src.models.weather import Weather
from src.models.gear import Gear
from src.models.split import Split
from src.models.zones import Zones
from src.database.db import (
    create_activities_table,
    create_gear_table,
    create_weather_table,
    create_splits_table,
    create_zones_table,
)
from src.database.queries import extract_and_compare_ids


def main():
    """Main function to fetch and process Strava activities and weather data."""
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
    create_splits_table()
    create_zones_table()

    # Fetch activities
    activities = strava_client.get_activities()
    activities_df = pd.DataFrame(activities)
    activities_df = process_activity_data(activities_df)

    # Insert new activities into the database and get their IDs
    new_activity_ids = Activity.process_activities(activities_df)
    if new_activity_ids:
        logger.info(f"New Activity IDs: {new_activity_ids}")

    # Get IDs of activities that are missing weather data
    missing_weather_ids = extract_and_compare_ids()
    if len(missing_weather_ids) > 0:
        logger.info(
            f"Fetching weather data for {len(missing_weather_ids)} missing activities."
        )
        # Filter activities_df to only include the missing activities
        activities_missing_weather = activities_df[
            activities_df["id"].isin(missing_weather_ids)
        ]

        # Fetch weather for activities that don't have it
        weather_client = WeatherClient(activities_missing_weather.copy())
        weather_df = weather_client.get_weather_data()
        weather_df = process_weather_data(weather_df)

        # Process and save new weather data to the database
        Weather.process_weather(weather_df)
    else:
        weather_df = pd.DataFrame()  # Empty weather DataFrame if no missing data

    if new_activity_ids:
        # Get unique gear IDs and process them
        gear_list = activities_df["gear_id"].dropna().unique().tolist()
        Gear.process_gears(strava_client, gear_list)

        # Process detailed activity data for new activities
        for activity_id in new_activity_ids:
            # Fetch detailed activity info
            detailed_activity = strava_client.get_detailed_activity(activity_id)
            logger.debug(f"Fetched detailed activity for ID {activity_id}")

            # Turn detailed activity into a DataFrame
            detailed_activity_df = pd.DataFrame([detailed_activity])

            # Extract splits data
            splits_df = extract_splits_data(detailed_activity_df)
            logger.debug(f"Extracted splits DataFrame for activity ID {activity_id}.")

            # Process and save splits to the database
            new_split_ids = Split.process_splits(splits_df)

            # Fetch zones data
            zones = strava_client.get_activity_zones(activity_id)
            if zones:
                zones_df = Zones.parse_activity_zones(zones, activity_id)
                logger.debug(f"Extracted zones DataFrame for activity ID {activity_id}.")

                # Process and save zones to the database
                Zones.process_zones(zones_df)
            else:
                logger.info(f"No zones data for activity {activity_id}")

    return activities_df, weather_df

    

if __name__ == "__main__":
    activities_df, weather_df = main()

