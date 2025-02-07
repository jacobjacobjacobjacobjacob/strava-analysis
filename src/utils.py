# src/utils.py
from datetime import datetime
from loguru import logger


def format_date(date_str):
    """Convert 'YYYY-MM-DDThh:mm:ss' into 'D. Mon YYYY' format."""
    date_part = date_str.split("T")[0]
    date_obj = datetime.strptime(date_part, "%Y-%m-%d")
    return f"{date_obj.day}. {date_obj.strftime('%b %Y')}"


def log_new_activity_details(activity_id, detailed_activity_df):
    """Logs key activity details such as name, date, sport type etc. for new activities"""
    # Filter the DataFrame to get the row for the specific activity id
    activity_row = detailed_activity_df.loc[
        detailed_activity_df["id"] == activity_id, ["name", "sport_type", "start_date"]
    ].iloc[0]

    name = activity_row["name"]
    sport_type = activity_row["sport_type"]
    date = activity_row["start_date"]
    date = format_date(date)

    logger.info(f"{name} - {date} - {sport_type}")
