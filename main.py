# main.py
import pandas as pd
from dotenv import load_dotenv
import os

from api.client import StravaClient
from models.processing import process_data

from models.activity import Activity
from database.db import create_activities_table

from models.gear import Gear
from database.db import create_gear_table

def main():
    load_dotenv()

    # Initialize Client
    client = StravaClient(
        client_id=os.getenv("STRAVA_CLIENT_ID"),
        client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
        refresh_token=os.getenv("STRAVA_REFRESH_TOKEN"),
        athlete_id=os.getenv("STRAVA_ATHLETE_ID"),
    )

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
    create_activities_table()
    create_gear_table()
    main()
    
