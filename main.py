import os
import sys
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

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
from src.database.queries import extract_and_compare_ids


def main():
    """ Main function to fetch and process Strava activities and weather data. """
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

    Activity.process_activities(activities_df)

    # Get unique gear IDs and process them
    gear_list = activities_df["gear_id"].dropna().unique().tolist()
    Gear.process_gears(strava_client, gear_list)

    return activities_df, weather_df


if __name__ == "__main__":
    activities_df, weather_df = main()

    # activity = client.get_detailed_activity(activity_id="12455871622")
    # zones = client.get_activity_zones(activity_id="12455871622")

# main.py
# import os
# import sys
# import pandas as pd
# from dotenv import load_dotenv
# from loguru import logger

# sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

# from src.clients.strava_client import StravaClient
# from src.clients.weather_client import WeatherClient
# from src.models.processing import process_activity_data, process_weather_data
# from src.models.activity import Activity
# from src.models.weather import Weather
# from src.models.gear import Gear
# from src.database.db import (
#     create_activities_table,
#     create_gear_table,
#     create_weather_table,
# )


# def main():
#     load_dotenv()
#     strava_client = StravaClient(
#         client_id=os.getenv("STRAVA_CLIENT_ID"),
#         client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
#         refresh_token=os.getenv("STRAVA_REFRESH_TOKEN"),
#         athlete_id=os.getenv("STRAVA_ATHLETE_ID"),
#     )

#     # Create tables
#     create_activities_table()
#     create_weather_table()
#     create_gear_table()

#     # Fetch activities
#     activities = strava_client.get_activities()
#     activities_df = pd.DataFrame(activities)
#     activities_df = process_activity_data(activities_df)

#     # Fetch weather for activities that don't have it
#     weather_client = WeatherClient(activities_df.copy())
#     weather_df = weather_client.get_weather_data()

#     # Only process weather data if there are new rows
#     if not weather_df.empty:
#         weather_df = process_weather_data(weather_df)
#         Weather.process_weather(weather_df)

#     # Process and save activities to the database
#     Activity.process_activities(activities_df)

#     # Get unique gear IDs and process them
#     gear_list = activities_df["gear_id"].dropna().unique().tolist()
#     Gear.process_gears(strava_client, gear_list)

#     return activities_df, weather_df


# if __name__ == "__main__":
#     activities_df, weather_df = main()

#     # activity = client.get_detailed_activity(activity_id="12455871622")
#     # zones = client.get_activity_zones(activity_id="12455871622")
# OLD WEATHER CLIENT
# # src/clients/weather_client.py
# import requests
# import pandas as pd
# from datetime import datetime, timedelta
# from loguru import logger
# from src.database.db import connect_weather_db, connect_activities_db


# class WeatherClient:
#     BASE_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast?"

#     def __init__(self, df: pd.DataFrame):
#         self.df = df
#         logger.info("Initializing WeatherClient")

#     def get_existing_weather_ids(self):
#         """Fetch activity IDs from the weather table that already have weather data."""
#         conn = connect_weather_db()
#         cursor = conn.cursor()
#         cursor.execute("SELECT id FROM weather")
#         existing_ids = cursor.fetchall()
#         conn.close()

#         # Convert the list of tuples to a flat list of activity IDs
#         return {row[0] for row in existing_ids}

#     def get_existing_activity_ids(self):
#         """Fetch activity IDs from the activities table."""
#         conn = connect_activities_db()
#         cursor = conn.cursor()
#         cursor.execute("SELECT id FROM activities")
#         activity_ids = cursor.fetchall()
#         conn.close()

#         # Convert the list of tuples to a flat list of activity IDs
#         return {row[0] for row in activity_ids}

#     def get_weather_data(self):
#         # Get all activities and weather entries
#         existing_activity_ids = self.get_existing_activity_ids()
#         existing_weather_ids = self.get_existing_weather_ids()

#         # Filter out activities that already have weather data
#         missing_weather_ids = list(
#             set(existing_activity_ids) - set(existing_weather_ids)
#         )

#         if not missing_weather_ids:
#             logger.info("All activities already have weather data.")
#             return pd.DataFrame()  # No new data to fetch

#         # Filter self.df to include only rows with missing weather data
#         df_missing_weather = self.df[self.df["id"].isin(missing_weather_ids)]

#         # Fetch weather data for those activities
#         df_missing_weather["weather_data"] = df_missing_weather.apply(
#             self.fetch_weather_data, axis=1, base_url=self.BASE_URL
#         )
#         logger.info(f"Fetched weather data for {len(df_missing_weather)} activities.")

#         # Return the dataframe with fetched weather data
#         return self.extract_weather_info(df_missing_weather)

#     def extract_weather_info(self, df):
#         """Extracts and expands weather information from the fetched data."""
#         weather_info = df.apply(self.extract_row_weather, axis=1).apply(pd.Series)
#         return pd.concat([df, weather_info], axis=1)


#     def round_time_to_nearest_hour(self, time_str):
#         # Convert the time string into a datetime object
#         time_obj = datetime.strptime(time_str, "%H:%M")

#         # Round to the nearest hour
#         if time_obj.minute >= 30:
#             # Add timedelta of one hour and set minutes to 00
#             time_obj = time_obj + timedelta(hours=1)

#         # Return the rounded time as a string with format HH:MM
#         return time_obj.strftime("%H:00")

#     def fetch_weather_data(self, row, base_url):
#         try:
#             lat, lng = row["lat_lng"].split(", ")
#         except ValueError:
#             logger.warning(
#                 f"Invalid lat_lng format in row {row.name}: {row['lat_lng']}"
#             )
#             return None

#         params = {
#             "latitude": lat,
#             "longitude": lng,
#             "start_date": row["date"],
#             "end_date": row["date"],
#             "hourly": ",".join(
#                 [
#                     "temperature_2m",
#                     "precipitation",
#                     "weather_code",
#                     "wind_speed_10m",
#                     "rain",
#                     "snowfall",
#                 ]
#             ),
#             "wind_speed_unit": "ms",
#         }

#         response = requests.get(base_url, params=params)
#         if response.status_code == 200:
#             return response.json()
#         else:
#             logger.warning(f"Failed for row {row.name}: {response.status_code}")
#             return None

#     def get_weather_data(self):
#         # Get all activities and weather entries
#         existing_activity_ids = self.get_existing_activity_ids()
#         existing_weather_ids = self.get_existing_weather_ids()

#         # Filter out activities that already have weather data
#         missing_weather_ids = list(
#             set(existing_activity_ids) - set(existing_weather_ids)
#         )

#         if not missing_weather_ids:
#             logger.info("All activities already have weather data.")
#             return pd.DataFrame()  # No new data to fetch

#         # Filter self.df to include only rows with missing weather data
#         df_missing_weather = self.df[self.df["id"].isin(missing_weather_ids)]

#         # Fetch weather data for those activities
#         df_missing_weather["weather_data"] = df_missing_weather.apply(
#             self.fetch_weather_data, axis=1, base_url=self.BASE_URL
#         )
#         logger.info(f"Fetched weather data for {len(df_missing_weather)} activities.")

#         # Return the dataframe with fetched weather data
#         return self.extract_weather_info(df_missing_weather)
