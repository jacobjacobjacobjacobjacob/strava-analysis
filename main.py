import os
import sys
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

# Add src directory to the system path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from utils import fetch_detailed_activity_with_retries
from src.clients.strava_client import StravaClient
from src.clients.weather_client import WeatherClient
from src.clients.streams_client import StreamClient 
from src.models.processing import process_activity_data, process_weather_data, extract_splits_data
from src.models.activity import Activity
from src.models.weather import Weather
from src.models.gear import Gear
from src.models.split import Split
from src.models.zones import Zones
from src.models.streams import Streams
from src.database.db import (
    create_activities_table,
    create_gear_table,
    create_weather_table,
    create_splits_table,
    create_zones_table,
    create_streams_table,
    insert_streams
)
from src.database.queries import extract_and_compare_ids


def main():
    """
    Main function to fetch and process Strava activities and weather data.
    """
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
    create_streams_table()

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
    if missing_weather_ids:
        logger.info(f"Fetching weather data for {len(missing_weather_ids)} missing activities.")
        activities_missing_weather = activities_df[activities_df["id"].isin(missing_weather_ids)]

        # Fetch weather for activities that don't have it
        weather_client = WeatherClient(activities_missing_weather.copy())
        try:
            weather_df = weather_client.get_weather_data()
        except Exception as e:
            logger.error(f"Error fetching weather data: {e}")
            weather_df = pd.DataFrame()  # Empty weather DataFrame if there's an error
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
            detailed_activity = fetch_detailed_activity_with_retries(strava_client, activity_id)
            if detailed_activity is None:
                continue
            detailed_activity_df = pd.DataFrame([detailed_activity])

            # Splits
            splits_df = extract_splits_data(detailed_activity_df)
            print(splits_df.tail())
            logger.debug(f"Extracted splits DataFrame for activity ID {activity_id}.")
            Split.process_splits(splits_df)

            # Zones
            zones = strava_client.get_activity_zones(activity_id)
            if zones:
                zones_df = Zones.parse_activity_zones(zones, activity_id)
                logger.debug(f"Extracted zones DataFrame for activity ID {activity_id}.")
                Zones.process_zones(zones_df)
            else:
                logger.info(f"No zones data for activity {activity_id}")

            # Streams
            
            # streams = stream_client.get_all_streams(activity_id=12992452963)
            # stream_instance = Streams(activity_id, streams)
            # streams_df = stream_instance.parse_streams_to_dataframe_compact(activity_id, streams)
         
            try:
                # Fetch all streams for the activity
                stream_client = StreamClient(strava_client)
                all_streams = stream_client.get_all_streams(activity_id)

                # Parse streams into a DataFrame
                streams_df = Streams.parse_streams_to_dataframe_compact(activity_id, all_streams)

                # Insert parsed streams into the database
                insert_streams(streams_df)
            except Exception as e:
                logger.error(f"Error processing streams for activity {activity_id}: {e}")




    return activities_df, weather_df


if __name__ == "__main__":
    activities_df, weather_df = main()


    
    # load_dotenv()
    # strava_client = StravaClient(
    #     client_id=os.getenv("STRAVA_CLIENT_ID"),
    #     client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
    #     refresh_token=os.getenv("STRAVA_REFRESH_TOKEN"),
    #     athlete_id=os.getenv("STRAVA_ATHLETE_ID"),
    # )

    # stream_client = StreamClient(strava_client)
    # activity_id = 12977344734
    

    # # available_streams = strava_client.get_activity_available_streams(activity_id)
    # # print("Available streams:", available_streams)

    # import sqlite3
    # import pandas as pd

    # def parse_streams(activity_id, streams_response):
    #     """
    #     Parses the streams response and structures it for database insertion.
        
    #     :param activity_id: The ID of the activity.
    #     :param streams_response: The API response containing stream data.
    #     :return: A structured DataFrame.
    #     """
    #     data = {"activity_id": activity_id}
    #     for stream_type, stream_data in streams_response.items():
    #         if isinstance(stream_data, dict) and "data" in stream_data:
    #             data[f"{stream_type}_stream"] = str(stream_data["data"])  # Store as a string
    #         else:
    #             data[f"{stream_type}_stream"] = None  # Handle missing streams
    #     return pd.DataFrame([data])


    # def parse_streams_to_dataframe(activity_id, streams_response):
    #     """Parses the streams response and structures it into a DataFrame for inspection."""
    #     # Dict to hold the parsed data
    #     parsed_data = {"activity_id": activity_id}

    #     # Loop through each stream type
    #     for stream_type, stream_data in streams_response.items():
    #         # Add stream data as a column
    #         if isinstance(stream_data, list):
    #             parsed_data[stream_type] = stream_data
    #         else:
    #             parsed_data[stream_type] = [None]  # Handle unexpected formats

    #     # Convert to DataFrame; handling lists of different lengths requires adjustment
    #     df = pd.DataFrame({key: pd.Series(value) for key, value in parsed_data.items()})

    #     # Keep activity_id as a constant column
    #     df["activity_id"] = activity_id

    #     # Drop latlng
    #     if "latlng" in df.columns:
    #         df.drop(columns=["latlng"], inplace=True)
    #     return df
    
    # def parse_streams_to_dataframe_compact(activity_id, streams_response):
    #     """
    #     Parses the streams response and structures it into a single-row DataFrame 
    #     with each stream's data wrapped into a list.
        
    #     :param activity_id: The ID of the activity.
    #     :param streams_response: The API response containing stream data.
    #     :return: A single-row DataFrame.
    #     """
    #     # Initialize the parsed data dictionary
    #     parsed_data = {"activity_id": int(activity_id)}

    #     # Process each stream
    #     for stream_type, stream_data in streams_response.items():
    #         # Wrap data in a list, or set to None if stream data is missing
    #         if isinstance(stream_data, list):
    #             parsed_data[stream_type] = stream_data
    #         else:
    #             parsed_data[stream_type] = None


    #     # Convert the dictionary into a single-row DataFrame
    #     return pd.DataFrame([parsed_data])




    # activity_id = 12977344734


    # # Fetch all available streams
    # all_streams = stream_client.get_all_streams(activity_id)

    # streams_df = parse_streams_to_dataframe_compact(activity_id, all_streams)
    # # pd.options.display.max_columns = 100
    # # print(streams_df.head())
    # # create_streams_table()
    # # insert_streams(streams_df)
    # # THIS WORKS FINE
    # import time

    # def get_ids_from_table(conn, table_name, column_name="id", date_column=None, year=None):
    #     """
    #     Fetches IDs from the specified table and column in the database, with optional year filtering.

    #     Args:
    #         conn (sqlite3.Connection): Database connection object.
    #         table_name (str): Name of the table to query.
    #         column_name (str): Name of the column to extract IDs from.
    #         date_column (str): Name of the column containing dates. Optional.
    #         year (int): Filter IDs by year. Optional.

    #     Returns:
    #         set: A set of IDs from the specified column, optionally filtered by year.
    #     """
    #     try:
    #         cursor = conn.cursor()
    #         if year and date_column:
    #             query = f"""
    #             SELECT {column_name} 
    #             FROM {table_name} 
    #             WHERE strftime('%Y', {date_column}) = '{year}';
    #             """
    #         else:
    #             query = f"SELECT {column_name} FROM {table_name};"
            
    #         cursor.execute(query)
    #         ids = {row[0] for row in cursor.fetchall()}
    #         logger.info(f"Extracted {len(ids)} IDs from {table_name} for year {year}.")
    #         return ids
    #     except sqlite3.Error as e:
    #         logger.error(f"Error querying {table_name}: {e}")
    #         return set()

    # def fetch_activity_ids_not_in_streams(year):
    #     """
    #     Fetch all activity IDs from activities.db for the specified year that are NOT present in streams.db.

    #     Args:
    #         year (int): The year to filter activities by.

    #     Returns:
    #         list: Activity IDs not present in streams.db for the specified year.
    #     """
    #     conn_activities = sqlite3.connect("data/activities.db")
    #     conn_streams = sqlite3.connect("data/streams.db")  # Adjust path as needed

    #     try:
    #         # Fetch IDs from activities table for the specified year
    #         activities_ids = get_ids_from_table(
    #             conn_activities,
    #             "activities",
    #             column_name="id",
    #             date_column="date",
    #             year=year,
    #         )

    #         # Fetch IDs from streams table (using activity_id column)
    #         streams_ids = get_ids_from_table(conn_streams, "streams", column_name="activity_id")

    #         # Find IDs present in activities but not in streams
    #         missing_ids = activities_ids - streams_ids

    #         logger.info(f"Found {len(missing_ids)} activity IDs not in streams.db for year {year}.")
    #         return list(missing_ids)

    #     except Exception as e:
    #         logger.error(f"Error comparing activity and streams IDs: {e}")
    #         return []

    #     finally:
    #         conn_activities.close()
    #         conn_streams.close()

    # def process_activities_in_batches(activity_ids, batch_size=50, wait_time=900):
    #     """
    #     Process activities in batches to fetch and store streams.

    #     Args:
    #         activity_ids (list): List of activity IDs to process.
    #         batch_size (int): Number of activities to process in each batch. Default is 50.
    #         wait_time (int): Time to wait between batches, in seconds. Default is 900 (15 minutes).
    #     """
    #     total_activities = len(activity_ids)
    #     logger.info(f"Starting batch processing for {total_activities} activities.")
        
    #     for i in range(0, total_activities, batch_size):
    #         batch = activity_ids[i : i + batch_size]
    #         logger.info(f"Processing batch {i // batch_size + 1} with {len(batch)} activities.")

    #         for activity_id in batch:
    #             try:
    #                 # Fetch streams for the activity
    #                 all_streams = stream_client.get_all_streams(activity_id)

    #                 # Parse the streams into a DataFrame
    #                 streams_df = parse_streams_to_dataframe_compact(activity_id, all_streams)

    #                 # Insert the streams into the database
    #                 insert_streams(streams_df)

    #             except Exception as e:
    #                 logger.error(f"Error processing activity ID {activity_id}: {e}")
            
    #         if i + batch_size < total_activities:
    #             logger.info(f"Waiting {wait_time} seconds before processing the next batch.")
    #             time.sleep(wait_time)

    # # Fetch missing activity IDs for the year 2024
    # missing_activity_ids = fetch_activity_ids_not_in_streams(year=2024)

    # # Process the activities in batches
    # process_activities_in_batches(missing_activity_ids)
