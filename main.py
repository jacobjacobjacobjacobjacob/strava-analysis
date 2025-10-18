# main.py
import pandas as pd
from loguru import logger
from src.api.strava_api.strava_api import StravaClient
from src.api.config import get_strava_api_config
from src.db.db_manager import DatabaseManager
from src.models.gear import Gear
from src.models.splits import Splits
from src.models.zones import Zones
from src.models.activity import Activity
from src.models.best_efforts import BestEfforts
from src.models.streams import Streams

from src.utils.logging import (
    log_new_activity_details,
    log_new_activities_count,
    log_new_gear,
)


def main():
    # Retrieves activities
    activities_data = strava_client.get_activities()

    # If no new activites are found
    if not activities_data:
        logger.error("Could not fetch data from Strava.")
        return

    # If data is recieved, turn to dataframe
    activities_df = pd.DataFrame(activities_data)

    # If dataframe is empty, no new activities to process
    if activities_df.empty:
        logger.warning("No new activities to process.")
        db_manager.check_strava_database_discrepancies()
        return

    try:
        # Process and filter activities
        logger.info("Processing activity data...")
        activities_df = Activity.process_activity_data(activities_df)

        # Retrieve all the cached ids
        cached_ids = db_manager.get_ids_from_cache()

        # Filter the dataframe for new activities and get a list of new activity IDs
        new_activities_df = activities_df[~activities_df["id"].isin(cached_ids)]

        new_activity_ids = new_activities_df["id"].tolist()

        # If there are new IDs
        if new_activity_ids:
            log_new_activities_count(new_activity_ids)
            process_new_activities(new_activity_ids, new_activities_df)
            process_gear_data(df=activities_df)

        else:
            logger.warning("No new activities.\n")

    except Exception as e:
        logger.error(f"Error during main processing: {e}")

    finally:
        db_manager.check_strava_database_discrepancies()


def process_gear_data(df: pd.DataFrame) -> None:
    gear_ids = db_manager.get_gear_ids()

    gear_df = Gear.process_gears(strava_client, df)

    new_gear_df = gear_df[~gear_df["gear_id"].isin(gear_ids)]

    if not new_gear_df.empty:
        log_new_gear(new_gear_df)

    db_manager.insert_dataframe_to_db(df=gear_df, table_name="gear")


def process_new_activities(new_activity_ids, new_activities_df):
    db_manager.insert_dataframe_to_db(df=new_activities_df, table_name="activities")
    for activity_id in new_activity_ids:
        db_manager.update_cache(activity_id)

        try:
            detailed_activity = strava_client.get_detailed_activity(activity_id)
            # print(detailed_activity)
            logger.debug(f"Processing activity id: {activity_id}")

            if not detailed_activity:
                logger.warning(f"Activity {activity_id} has no detailed data.")
                continue

            # Process the detailed activity data if it exists
            process_individual_activity(activity_id, detailed_activity)

        except Exception as e:
            logger.error(f"Error processing activity {activity_id}: {e}")


def process_individual_activity(activity_id: int, detailed_activity):
    try:
        detailed_activity_df = pd.DataFrame([detailed_activity])
        splits_df = Splits.process_splits(strava_client, detailed_activity_df)
        zones_data = strava_client.get_activity_zones(activity_id)
        zones_df = Zones.process_zones(zones_data, activity_id)
        best_efforts_data = detailed_activity.get("best_efforts", [])
        best_efforts_df = BestEfforts.process_best_efforts(
            activity_id, best_efforts_data
        )
        streams_df = Streams.get_streams(strava_client, activity_id)

        log_new_activity_details(activity_id, detailed_activity_df)

        # Insert processed data into the database
        db_manager.insert_dataframe_to_db(df=splits_df, table_name="splits")
        db_manager.insert_dataframe_to_db(df=zones_df, table_name="zones")
        db_manager.insert_dataframe_to_db(df=best_efforts_df, table_name="best_efforts")
        db_manager.insert_dataframe_to_db(df=streams_df, table_name="streams")

    except Exception as e:
        logger.error(f"Error in processing individual activity {activity_id}: {e}")


if __name__ == "__main__":
    strava_client = StravaClient(**get_strava_api_config())

    db_manager = DatabaseManager()
    # db_manager.delete_last_activity()  # Clear the last activity from the cache for debugging
    db_manager.create_all_tables()
    
    main()

    db_manager.export_table_to_csv(table_name="activities")  
