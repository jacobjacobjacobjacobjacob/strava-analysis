# src/database/queries.py
import sqlite3
import pandas as pd
from loguru import logger
from src.database.db import connect_activities_db, connect_weather_db


def get_table_data(table_name: str, db_name: str):
    """Retrieves all the data from a given table as a Pandas DataFrame."""
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(f"data/db_name")

        # Query to get all data from the specified table
        query = f"SELECT * FROM {table_name}"

        # Load the data into a DataFrame
        df = pd.read_sql_query(query, conn)

        return df
    except sqlite3.Error as e:
        logger.error(f"Error retrieving table data: {e}")
        return None
    finally:
        if conn:
            conn.close()


def extract_and_compare_ids():
    """Extracts ids from activities and weather databases, and returns ids present in activities but not in weather."""

    def extract_ids(conn, table_name):
        """Extracts the ids from a given table in a database using an existing connection."""
        try:
            cursor = conn.cursor()
            query = f"SELECT id FROM {table_name};"
            cursor.execute(query)
            ids = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Extracted {len(ids)} IDs from the {table_name} table.")
        except sqlite3.Error as e:
            logger.error(f"Error extracting IDs from {table_name} table: {e}")
            ids = []
        return ids

    # Connect to both databases
    conn_activities = connect_activities_db()
    conn_weather = connect_weather_db()

    try:
        # Extract ids from both databases
        activities_ids = extract_ids(conn_activities, "activities")
        weather_ids = extract_ids(conn_weather, "weather")

        # Compare and return ids in activities but not in weather
        diff_ids = list(set(activities_ids) - set(weather_ids))

    finally:
        # Close the connections
        conn_activities.close()
        conn_weather.close()

    if len(diff_ids) == 0:
        logger.info("All activity IDs are present in the weather database.")

    else:
        logger.info(
            f"Found {len(diff_ids)} IDs present in activities but not in weather."
        )
        logger.debug(f"Missing IDs: {diff_ids}")


    return diff_ids

# Query to join the databses for later:

# SELECT a.*, w.temperature, w.precipitation

# FROM activities a
# JOIN weather w ON a.activity_id = w.activity_id;
