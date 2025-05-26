# src/utils/logging.py
from datetime import datetime
import pandas as pd
from loguru import logger



def log_new_activities_count(new_activity_ids: list) -> None:
    """Logs the number of new activities found"""
    new_activities_count = len(new_activity_ids)
    if new_activities_count == 0:
        logger.info("No new activities found.")
    else:
        # Use pluralization to handle both singular and plural cases dynamically
        activity_word = "activity" if new_activities_count == 1 else "activities"
        logger.info(f"{new_activities_count} new {activity_word} found.")


def log_new_activity_details(activity_id: int, detailed_activity_df) -> None:
    """Logs key activity details such as name, date, and sport type for new activities"""
    # Check if the activity_id exists in the dataframe
    activity_row = detailed_activity_df[detailed_activity_df["id"] == activity_id]
    if activity_row.empty:
        logger.warning(f"Activity {activity_id} not found in the dataframe.")
        return

    name = activity_row["name"].values[0]
    sport_type = activity_row["sport_type"].values[0]
    date = activity_row["start_date"].values[0]
    distance = activity_row["distance"].values[0]

    def format_date(date_str):
        """Convert 'YYYY-MM-DDThh:mm:ss' into 'D. Mon YYYY' format."""
        date_part = date_str.split("T")[0]
        date_obj = datetime.strptime(date_part, "%Y-%m-%d")
        return f"{date_obj.day}. {date_obj.strftime('%b %Y')}"

    date = format_date(date)

    logger.info(f"- {date} - {sport_type} - {name}\n")


def log_new_gear(new_gear_df: pd.DataFrame) -> bool:
    """Check if new gear is detected and determine gear type."""

    # Get the gear IDs from the new gear DataFrame
    new_gear_ids = new_gear_df["gear_id"].tolist()

    def format_gear_logging_message():
        logger.info("New gear detected!")
        # Iterate over each gear_id and determine gear type based on the first letter
        for index, row in new_gear_df.iterrows():
            gear_id = row["gear_id"]
            name = row["name"]
            distance = row["distance"]

            # Determine gear type based on gear_id
            if gear_id.startswith("b"):
                gear_type = "Bike"
            elif gear_id.startswith("g"):
                gear_type = "Shoes"
            else:
                gear_type = "Unknown"

            # Log the gear info with gear type, name, and distance (converted to km)
            logger.info(f"{gear_type} - {name} - {round(distance / 1000)} km")

    if len(new_gear_ids) > 0:
        format_gear_logging_message()

    else:
        return
