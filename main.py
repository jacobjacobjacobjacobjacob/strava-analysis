# main.py
import time
import pandas as pd
from loguru import logger
from src.api.strava_api import StravaClient
from src.api.weather_api import WeatherClient
from src.db import DatabaseManager
from src.models.weather import Weather
from src.models.gear import Gear
from src.models.splits import Splits
from src.models.zones import Zones
from src.models.activity import Activity
from src.models.best_efforts import BestEfforts
from src.config import get_strava_config


def main():
    activities_data = strava_client.get_activities()
    if not activities_data:
        logger.warning("No activities data fetched from Strava.")
        db_manager.check_discrepancies()
        return

    activities_df = pd.DataFrame(activities_data)
    if activities_df.empty:
        logger.warning("No new activities to process.")
        db_manager.check_discrepancies()
        return

    try:
        # Process and filter activities
        activities_df = Activity.process_activity_data(activities_df)
        cached_ids = db_manager.get_ids_from_cache()
        new_activities_df = activities_df[~activities_df["id"].isin(cached_ids)]
        new_activity_ids = new_activities_df["id"].tolist()

        if new_activity_ids:
            # Insert new activities into the database
            db_manager.insert_dataframe_to_db(df=new_activities_df, table_name="activities")

            # Process each new activity in detail
            process_new_activities(new_activity_ids)

    except Exception as e:
        logger.error(f"Error during main processing: {e}")

    finally:
        db_manager.check_discrepancies()


def process_new_activities(new_activity_ids):
    for activity_id in new_activity_ids:
        try:
            logger.debug(f"Processing activity {activity_id}")
            detailed_activity = strava_client.get_detailed_activity(activity_id)

            if not detailed_activity:
                logger.warning(f"Activity {activity_id} has no detailed data.")
                continue

            # Process detailed data
            process_individual_activity(activity_id, detailed_activity)

        except Exception as e:
            logger.error(f"Error processing activity {activity_id}: {e}")


def process_individual_activity(activity_id, detailed_activity):
    try:
        detailed_activity_df = pd.DataFrame([detailed_activity])
        splits_df = Splits.process_splits(strava_client, detailed_activity_df)
        zones_data = strava_client.get_activity_zones(activity_id)
        zones_df = Zones.process_zones(zones_data, activity_id)
        best_efforts_data = detailed_activity.get("best_efforts", [])
        best_efforts_df = BestEfforts.process_best_efforts(activity_id, best_efforts_data)

        # Insert processed data into the database
        db_manager.insert_dataframe_to_db(df=splits_df, table_name="splits")
        db_manager.insert_dataframe_to_db(df=zones_df, table_name="zones")
        db_manager.insert_dataframe_to_db(df=best_efforts_df, table_name="best_efforts")

    except Exception as e:
        logger.error(f"Error in processing individual activity {activity_id}: {e}")



if __name__ == "__main__":
    strava_client = StravaClient(**get_strava_config())
    # weather_client = WeatherClient()

    db_manager = DatabaseManager()
    db_manager.create_all_tables()
    main()

    


    lat = "52.52"
    lng = "13.41"
    date = "2025-01-01"
    # weather = weather_client.get_weather_data(activity_id=13263470530)

    import json

    # print(json.dumps(weather, indent=4))
    # print(weather)

    # Fetch row from db with params
    # from src.queries import get_weather_params

    # print("ID:")
    # print(id)

    # print("DATE:")
    # print(date)

    # print("LAT_LNG:")
    # print(lat_lng)

    # print("TIME:")
    # print(time)
