# src/database/queries.py
import sqlite3
import time
import pandas as pd
from loguru import logger
from src.database.db import (
    connect_activities_db,
    connect_weather_db,
    connect_splits_db,
    connect_zones_db,
)

DATABASE_PATHS = {
    "zones": "data/zones.db",
    "activities": "data/activities.db",
    "gear": "data/gear.db",
    "weather": "data/weather.db",
    "splits": "data/splits.db",
}


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


def get_all_activities(db_path="data/activities.db"):
    """Retrieve all activities from the database."""
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM activities"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_all_weather_data(db_path="data/weather.db"):
    """Retrieve all weather data from the database."""
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM weather"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def extract_and_compare_ids():
    """Extracts ids from activities and weather databases, and returns ids present in activities but not in weather."""

    def extract_ids(conn, table_name):
        """Extracts the ids from a given table in a database using an existing connection."""
        try:
            cursor = conn.cursor()
            query = f"SELECT id FROM {table_name};"
            cursor.execute(query)
            ids = [row[0] for row in cursor.fetchall()]
            logger.info(f"Extracted {len(ids)} IDs from the {table_name} table.")
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


def get_sport_type_ids(db_name, table_name, sport_type):
    """Retrieves a list of IDs from the specified table where sport_type matches."""
    try:
        # Connect to the activities database
        conn = connect_activities_db()
        cursor = conn.cursor()

        # Prepare and execute the query
        query = f"SELECT id FROM {table_name} WHERE sport_type = ?"
        cursor.execute(query, (sport_type,))

        # Fetch and return the IDs
        ids = [row[0] for row in cursor.fetchall()]
        logger.debug(
            f"Extracted {len(ids)} IDs from the {table_name} table for sport_type '{sport_type}'."
        )
        return ids
    except sqlite3.Error as e:
        logger.error(f"Error retrieving IDs for sport_type '{sport_type}': {e}")
        return []
    finally:
        if conn:
            conn.close()


def fetch_all_ids(year: int = None):
    """Fetch all activity IDs from the activities table. Optionally filter by year."""
    conn = connect_activities_db()
    cursor = conn.cursor()

    # Query to fetch IDs for the specified year
    query = """
            SELECT id 
            FROM activities 
            WHERE date LIKE ? 
            ORDER BY id ASC
        """
    cursor.execute(query, (f"{year}%",))

    ids = [row[0] for row in cursor.fetchall()]

    conn.close()
    return ids


def compare_ids_and_output():
    """
    Compare IDs from activities.db and splits.db and output the count of missing IDs.
    """
    conn_activities = connect_activities_db()
    conn_splits = connect_splits_db()

    try:
        # Get IDs from both databases
        activities_ids = get_ids_from_table(conn_activities, "activities")
        splits_ids = get_ids_from_table(conn_splits, "splits")

        # Find IDs in activities but not in splits
        missing_ids = activities_ids - splits_ids

        logger.info(f"Total activities IDs: {len(activities_ids)}")
        logger.info(f"Total splits IDs: {len(splits_ids)}")
        logger.info(f"Missing IDs in splits.db: {len(missing_ids)}")
        logger.debug(f"Missing IDs: {missing_ids}")

    finally:
        conn_activities.close()
        conn_splits.close()


def get_ids_from_table(conn, table_name, column_name="id"):
    """
    Fetches all IDs from the specified table and column in the database.

    Args:
        conn (sqlite3.Connection): Database connection object.
        table_name (str): Name of the table to query.
        column_name (str): Name of the column to extract IDs from.

    Returns:
        set: A set of IDs from the specified column.
    """
    try:
        cursor = conn.cursor()
        query = f"SELECT {column_name} FROM {table_name};"
        cursor.execute(query)
        ids = {row[0] for row in cursor.fetchall()}
        logger.info(f"Extracted {len(ids)} IDs from {table_name}.")
        return ids
    except sqlite3.Error as e:
        logger.error(f"Error querying {table_name}: {e}")
        return set()

def fetch_activity_ids_not_in_streams():
    """
    Fetch all activity IDs from activities.db that are NOT present in streams.db.

    Returns:
        list: Activity IDs not present in streams.db.
    """
    conn_activities = connect_activities_db()
    conn_streams = sqlite3.connect("data/streams.db")  # Adjust path as needed

    try:
        # Fetch IDs from activities table
        activities_ids = get_ids_from_table(conn_activities, "activities", column_name="id")

        # Fetch IDs from streams table (using activity_id column)
        streams_ids = get_ids_from_table(conn_streams, "streams", column_name="activity_id")

        # Find IDs present in activities but not in streams
        missing_ids = activities_ids - streams_ids

        logger.info(f"Found {len(missing_ids)} activity IDs not in streams.db.")
        return list(missing_ids)

    except Exception as e:
        logger.error(f"Error comparing activity and streams IDs: {e}")
        return []

    finally:
        conn_activities.close()
        conn_streams.close()


def process_activities_in_batches(activity_ids, batch_size=50, wait_time=900):

        """
        Process activities in batches.

        Args:
            activity_ids (list): List of activity IDs to process.
            batch_size (int): Number of activities to process in each batch. Default is 60.
            wait_time (int): Time to wait between batches, in seconds. Default is 900 (15 minutes).
        """
        total_activities = len(activity_ids)
        for start_idx in range(0, total_activities, batch_size):
            # Get the current batch
            batch = activity_ids[start_idx:start_idx + batch_size]
            logger.info(f"Processing batch {start_idx // batch_size + 1} of {len(activity_ids) // batch_size + 1}...")
            
            # Process each activity in the batch
            for activity_id in batch:
                try:
                    # Fetch heartrate stream data
                    stream_data = streams_client.get_heartrate_stream(activity_id)
                    insert_stream_to_db(activity_id, stream_data)
                    logger.info(f"Inserted streams data for activity ID {activity_id}")
                except Exception as e:
                    logger.error(f"Error processing activity ID {activity_id}: {e}")

            # Wait before processing the next batch (if not the last batch)
            if start_idx + batch_size < total_activities:
                logger.info(f"Batch complete. Waiting {wait_time // 60} minutes before next batch...")
                time.sleep(wait_time)