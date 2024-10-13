# main.py
import os
import pandas as pd
from dotenv import load_dotenv
from src.client.client import StravaClient
from src.models.processing import process_data
from src.models.activity import Activity
from src.models.gear import Gear
from src.database.db import create_activities_table, create_gear_table
from src.database.queries import get_table_data


def main():
    load_dotenv()

    # Fetch activities
    activities = client.get_activities()
    activities_df = pd.DataFrame(activities)
    df = process_data(activities_df)


    # Process and save activities to the database
    Activity.process_activities(df)

    # Get unique gear IDs and process them
    gear_list = df["gear_id"].dropna().unique().tolist()
    Gear.process_gears(client, gear_list)


if __name__ == "__main__":

    load_dotenv()

    # Initialize Client
    client = StravaClient(
        client_id=os.getenv("STRAVA_CLIENT_ID"),
        client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
        refresh_token=os.getenv("STRAVA_REFRESH_TOKEN"),
        athlete_id=os.getenv("STRAVA_ATHLETE_ID"),
    )
    create_activities_table()
    # create_gear_table()
    main()

    # activity = client.get_detailed_activity(activity_id="12455871622")
    # zones = client.get_activity_zones(activity_id="12455871622")
