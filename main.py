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
from src.database.db import (
    create_activities_table,
    create_gear_table,
    create_weather_table,
    create_splits_table,
    insert_split
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

    # Fetch activities
    activities = strava_client.get_activities()
    activities_df = pd.DataFrame(activities)
    activities_df = process_activity_data(activities_df)

    # Insert new activities into the database and get their IDs
    new_activity_ids = Activity.process_activities(activities_df)
    logger.info(f"New Activity IDs: {new_activity_ids}")
    
    # Get IDs of activities that are missing weather data
    missing_weather_ids = (
        extract_and_compare_ids()
    )  # Compare activities and weather database IDs

    if len(missing_weather_ids) == 0:
        weather_df = pd.DataFrame()  # Empty weather DataFrame if no missing data
    else:
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


    # Get unique gear IDs and process them
    gear_list = activities_df["gear_id"].dropna().unique().tolist()
    Gear.process_gears(strava_client, gear_list)

    # Process detailed activity data for new activities
    if new_activity_ids:
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



    return activities_df, weather_df

    

if __name__ == "__main__":
    # activities_df, weather_df = main()
    pd.options.display.max_columns = None

    load_dotenv()
    strava_client = StravaClient(
        client_id=os.getenv("STRAVA_CLIENT_ID"),
        client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
        refresh_token=os.getenv("STRAVA_REFRESH_TOKEN"),
        athlete_id=os.getenv("STRAVA_ATHLETE_ID"),
    )

    # Zones testing
    from src.database.db import connect_zones_db, create_zones_table
    from src.database.queries import fetch_all_ids
    import time
    from src.models.zones import Zones


    create_zones_table()
    # FASIT: DETTE VIRKER 
    # activity_id = 12941426293
    # zones = strava_client.get_activity_zones(activity_id)
    # zones_df = Zones.parse_activity_zones(zones, activity_id)

    # Zones.process_zones(zones_df)

    # print("Zones successfully processed and saved.")

    def fetch_data_and_append_ids(ids, strava_client, data_type="zones"):
        """
        Fetch data (zones) for each activity ID and append the activity ID to each row.
        
        Args:
            ids (list): List of activity IDs.
            strava_client: Strava client to fetch data.
            data_type (str): "zones" to specify the data to fetch.
                
        Returns:
            pd.DataFrame: Combined DataFrame of fetched data.
        """
        all_data = []
        
        for activity_id in ids:
            try:
                # Fetch zones data
                data = strava_client.get_activity_zones(activity_id)
                if not data:
                    print(f"No {data_type} data for activity {activity_id}")
                    continue

                # Parse and normalize data
                data_df = pd.json_normalize(data)
                data_df["activity_id"] = activity_id  # Add activity_id to each row
                
                # Append to the master list
                all_data.append(data_df)
            
            except Exception as e:
                print(f"Error fetching {data_type} for activity {activity_id}: {e}")
        
        # Combine all DataFrames into one
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
        else:
            combined_df = pd.DataFrame()  # Return empty DataFrame if no data

        return combined_df

    def process_zones_for_activities(ids, strava_client, batch_size=90, wait_time=900):
        """
        Process zones for activity IDs in batches.

        Args:
            ids (list): List of activity IDs from 2024.
            strava_client: Strava client to fetch zones data.
            batch_size (int): Number of IDs to process per batch.
            wait_time (int): Time to wait between batches, in seconds.
        """
        create_zones_table()  # Ensure the zones table exists
        total_batches = (len(ids) + batch_size - 1) // batch_size  # Calculate total number of batches

        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            print(f"Processing batch {i // batch_size + 1} of {total_batches} for zones...")

            for activity_id in batch:
                try:
                    # Fetch zones data for the activity
                    zones = strava_client.get_activity_zones(activity_id)
                    if not zones:
                        print(f"No zones data for activity {activity_id}")
                        continue

                    # Parse zones into a DataFrame
                    zones_df = Zones.parse_activity_zones(zones, activity_id)

                    # Save zones to the database
                    Zones.process_zones(zones_df)

                except Exception as e:
                    print(f"Error processing zones for activity {activity_id}: {e}")

            # Wait for the next batch if there are remaining IDs
            if i + batch_size < len(ids):
                print(f"Batch {i // batch_size + 1} complete. Waiting {wait_time // 60} minutes...")
                time.sleep(wait_time)

        print("All zones successfully processed and saved.")

    def get_all_zones_data(strava_client):
        """
        Fetch and process zones data for all activities from 2024.

        Args:
            strava_client: Strava client to fetch zones data.
        """
        # Fetch all activity IDs from 2024
        all_ids = fetch_all_ids(2024)
        print(f"Total activity IDs fetched for 2024: {len(all_ids)}")

        # Process zones for all activity IDs
        process_zones_for_activities(all_ids, strava_client, batch_size=90, wait_time=900)

    # Run the function
    get_all_zones_data(strava_client)