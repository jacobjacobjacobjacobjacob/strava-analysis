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
    activities_df, weather_df = main()
    pd.options.display.max_columns = None

    # load_dotenv()
    # strava_client = StravaClient(
    #     client_id=os.getenv("STRAVA_CLIENT_ID"),
    #     client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
    #     refresh_token=os.getenv("STRAVA_REFRESH_TOKEN"),
    #     athlete_id=os.getenv("STRAVA_ATHLETE_ID"),
    # )

    # from src.database.db import connect_activities_db

    # """ FETCH ALL"""
    # import time

    # from database.queries import fetch_all_ids
        

    # import pandas as pd


    # from src.models.processing import extract_splits_data
    # df = pd.read_csv("all_detailed_activities.csv")
    # df = extract_splits_data(df)
    # create_splits_table()
    # from src.database.db import insert_split
    # for _, row in df.iterrows():
    #     insert_split(row)

# import time

# def fetch_and_save_detailed_activities(ids, strava_client, output_csv="all_detailed_activities.csv", batch_size=75, wait_time=900):
#     """
#     Fetch detailed activity data for each ID and save it to a CSV file.
    
#     Args:
#         ids (list): List of activity IDs.
#         strava_client: Strava client instance.
#         output_csv (str): Path to save the output CSV.
#         batch_size (int): Number of IDs to process per batch (default: 75).
#         wait_time (int): Time to wait between batches, in seconds (default: 900).
#     """
#     all_detailed_activities = []

#     for i in range(0, len(ids), batch_size):
#         batch = ids[i:i + batch_size]
#         print(f"Processing batch {i // batch_size + 1} of {len(ids) // batch_size + 1}...")
        
#         for activity_id in batch:
#             try:
#                 # Fetch detailed activity
#                 detailed_activity = strava_client.get_detailed_activity(activity_id)
#                 all_detailed_activities.append(detailed_activity)
#             except Exception as e:
#                 print(f"Error fetching activity ID {activity_id}: {e}")

#         # Save batch data to CSV
#         if all_detailed_activities:
#             pd.DataFrame(all_detailed_activities).to_csv(output_csv, index=False)
        
#         # Wait for the next batch if there are remaining IDs
#         if i + batch_size < len(ids):
#             print(f"Batch {i // batch_size + 1} complete. Waiting {wait_time // 60} minutes...")
#             time.sleep(wait_time)

#     print(f"All activities processed and saved to {output_csv}")


    
# all_ids = fetch_all_ids(2024)
# fetch_and_save_detailed_activities(all_ids, strava_client)



    # def fetch_data_and_append_ids(ids, strava_client, data_type="zones"):
    #     """
    #     Fetch data (zones or laps) for each activity ID and append the activity ID to each row.
        
    #     Args:
    #         ids (list): List of activity IDs.
    #         strava_client: Strava client to fetch data.
    #         data_type (str): Either "zones" or "laps" to specify the data to fetch.
            
    #     Returns:
    #         pd.DataFrame: Combined DataFrame of fetched data.
    #     """
    #     all_data = []
        
    #     for activity_id in ids:
    #         try:
    #             # Fetch data based on the data type
    #             if data_type == "zones":
    #                 data = strava_client.get_activity_zones(activity_id)
    #             elif data_type == "laps":
    #                 data = strava_client.get_activity_laps(activity_id)
    #             else:
    #                 raise ValueError(f"Invalid data_type: {data_type}")
                
    #             if not data:
    #                 print(f"No {data_type} data for activity {activity_id}")
    #                 continue

    #             # Parse and normalize data
    #             data_df = pd.json_normalize(data)
    #             data_df["activity_id"] = activity_id  # Add activity_id to each row
                
    #             # Append to the master list
    #             all_data.append(data_df)
            
    #         except Exception as e:
    #             print(f"Error fetching {data_type} for activity {activity_id}: {e}")
        
    #     # Combine all DataFrames into one
    #     if all_data:
    #         combined_df = pd.concat(all_data, ignore_index=True)
    #     else:
    #         combined_df = pd.DataFrame()  # Return empty DataFrame if no data

    #     return combined_df

    # def process_ids_in_batches(ids, strava_client, data_type, batch_size=25, wait_time=900):
    #     """
    #     Process activity IDs in batches for the specified data type (zones or laps).
        
    #     Args:
    #         ids (list): List of activity IDs.
    #         strava_client: Strava client to fetch data.
    #         data_type (str): Either "zones" or "laps".
    #         batch_size (int): Number of IDs to process per batch.
    #         wait_time (int): Time to wait between batches, in seconds.
            
    #     Returns:
    #         pd.DataFrame: Combined DataFrame of all fetched data.
    #     """
    #     all_batches_data = []
    #     total_batches = (len(ids) + batch_size - 1) // batch_size  # Calculate total number of batches

    #     for i in range(0, len(ids), batch_size):
    #         batch = ids[i:i + batch_size]
    #         print(f"Processing batch {i // batch_size + 1} of {total_batches} for {data_type}...")

    #         # Fetch data for the batch
    #         batch_data = fetch_data_and_append_ids(batch, strava_client, data_type)
    #         if not batch_data.empty:
    #             all_batches_data.append(batch_data)
            
    #         # Wait for the next batch if there are remaining IDs
    #         if i + batch_size < len(ids):
    #             print(f"Batch {i // batch_size + 1} for {data_type} complete. Waiting {wait_time // 60} minutes...")
    #             time.sleep(wait_time)
        
    #     # Combine all batches into a single DataFrame
    #     if all_batches_data:
    #         combined_df = pd.concat(all_batches_data, ignore_index=True)
    #     else:
    #         combined_df = pd.DataFrame()  # Return empty DataFrame if no data

    #     return combined_df

    # def save_to_csv(df, file_name):
    #     """Save the final DataFrame to a CSV file."""
    #     if not df.empty:
    #         df.to_csv(file_name, index=False)
    #         print(f"Saved data to {file_name}")
    #     else:
    #         print(f"No data to save for {file_name}")

    # def get_all_zones_laps_data(strava_client):
    #     # Fetch all activity IDs
    #     all_ids = fetch_all_ids()
    #     print(f"Total IDs fetched: {len(all_ids)}")
        
    #     # Process and save zones data
    #     #zones_df = process_ids_in_batches(all_ids, strava_client, data_type="zones")
    #     #save_to_csv(zones_df, "all_zones.csv")
        
    #     # Process and save laps data
    #     laps_df = process_ids_in_batches(all_ids, strava_client, data_type="laps")
    #     save_to_csv(laps_df, "all_laps.csv")

    
    # Call the main function with your strava_client
    #get_all_zones_laps_data(strava_client)

""" FROM NOW ON: GET IT DIRECTLY WHEN ADDING NEW ACTIVITIES TO THE DATABASE. INCLDUE A STEP TO FETCH THE ZONE AND WRITE IT TO THE CSV WITHOUT OVERWRITING """