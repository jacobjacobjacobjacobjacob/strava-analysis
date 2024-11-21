# src/database/db.py
import os
import sqlite3
import json
import pandas as pd
from loguru import logger


def connect_db(db_name):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    db_path = os.path.join(project_root, "data", db_name)
    conn = sqlite3.connect(db_path)
    return conn

def connect_activities_db():
    return connect_db("activities.db")

def connect_gear_db():
    return connect_db("gear.db")

def connect_weather_db():
    return connect_db("weather.db")

def connect_laps_db():
    return connect_db("laps.db")

def connect_splits_db():
    return connect_db("splits.db")

def connect_zones_db():
    return connect_db("zones.db")

def create_activities_table():
    conn = connect_activities_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS activities (
                   id INTEGER PRIMARY KEY,
                   date TEXT,
                   month TEXT,
                   day_of_week TEXT,
                   start_time TEXT,
                   end_time TEXT,
                   sport_type TEXT,
                   indoor INTEGER,
                   distance REAL,
                   duration REAL,
                   elevation_gain REAL,
                   gear_id TEXT,
                   average_heartrate REAL,
                   average_speed REAL,
                   average_cadence REAL,
                   average_temp REAL,
                   average_watts REAL,
                   intensity INTEGER,
                   lat_lng TEXT

                   )            
    """
    )
    conn.commit()
    conn.close()

def create_gear_table():
    conn = connect_gear_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS gear (
            gear_id TEXT PRIMARY KEY,
            name TEXT,
            distance REAL,
            brand_name TEXT,
            model_name TEXT,
            retired INTEGER,
            weight REAL
        )
        """
    )
    conn.commit()
    conn.close()

def create_weather_table():
    conn = connect_weather_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS weather (
                   id INTEGER PRIMARY KEY,
                   date TEXT,
                   temp REAL,
                   weather_type TEXT,
                   precipitation REAL,
                   rain REAL,
                   wind REAL,
                   snow REAL
                   )            
    """
    )
    conn.commit()
    conn.close()

def create_laps_table():
    conn = connect_laps_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS laps (
            id INTEGER,
            lap_name TEXT,
            duration REAL,
            date TEXT,
            distance REAL,
            average_speed REAL,
            lap_index INTEGER,
            split INTEGER,
            elevation_gain REAL,
            average_heartrate REAL,
            pace_zone TEXT,
            PRIMARY KEY (id, lap_index)
        )
        """
    )
    conn.commit()
    conn.close()

def create_splits_table():
    """
    Create the splits table in the splits.db database.
    """
    conn = connect_splits_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS splits (
            id INTEGER PRIMARY KEY,
            sport_type TEXT,
            splits_metric TEXT,
            laps TEXT,
            available_zones TEXT
        )
        """
    )
    conn.commit()
    conn.close()

def create_zones_table():
    """
    Create the zones table in the zones.db database.
    """
    conn = connect_zones_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS zones (
            activity_id INTEGER,
            zone_type TEXT,
            min_value INTEGER,
            max_value INTEGER,
            time_in_zone REAL,
            PRIMARY KEY (activity_id, zone_type, min_value, max_value)
        )
        """
    )
    conn.commit()
    conn.close()

def insert_activity(activity):
    conn = connect_activities_db()
    cursor = conn.cursor()

    # Check if the activity already exists
    cursor.execute("SELECT 1 FROM activities WHERE id = ?", (activity.activity_id,))
    existing_activity = cursor.fetchone()

    if not existing_activity:
        # Insert the activity
        cursor.execute(
            """
            INSERT INTO activities (id, date, month, day_of_week, start_time, end_time, 
                sport_type, indoor, distance, duration, elevation_gain, 
                gear_id, average_heartrate, average_speed, average_cadence, 
                average_temp, average_watts, intensity, lat_lng)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                activity.activity_id,
                activity.date,
                activity.month,
                activity.day_of_week,
                activity.start_time,
                activity.end_time,
                activity.sport_type,
                int(activity.indoor),
                activity.distance,
                activity.duration,
                activity.elevation_gain,
                activity.gear_id,
                activity.average_heartrate,
                activity.average_speed,
                activity.average_cadence,
                activity.average_temp,
                activity.average_watts,
                activity.intensity,
                activity.lat_lng,
            ),
        )
        conn.commit()
        conn.close()
        logger.info(f"Inserted new activity {activity.activity_id} into the database.")
        return True  # Activity was inserted
    else:
        conn.close()
        return False  # Activity already exists




def insert_gear(gear):
    from models.gear import Gear

    conn = connect_gear_db()
    cursor = conn.cursor()
    # Check if gear already exists
    cursor.execute("SELECT * FROM gear WHERE gear_id = ?", (gear.gear_id,))
    existing_gear = cursor.fetchone()

    if existing_gear:
        cursor.execute(
            """
            UPDATE gear
            SET name = ?, distance = ?, brand_name = ?, model_name = ?, retired = ?, weight = ?
            WHERE gear_id = ?
            """,
            (
                gear.name,
                gear.distance,
                gear.brand_name,
                gear.model_name,
                int(gear.retired),
                gear.weight,
                gear.gear_id,
            ),
        )
        logger.info(f"Updated gear: {gear.name}")
    else:
        cursor.execute(
            """
            INSERT INTO gear (gear_id, name, distance, brand_name, model_name, retired, weight)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gear.gear_id,
                gear.name,
                gear.distance,
                gear.brand_name,
                gear.model_name,
                int(gear.retired),
                gear.weight,
            ),
        )
        logger.info(f"Inserted gear: {gear.name}")

    conn.commit()
    conn.close()

def insert_weather(weather):
    conn = connect_weather_db()
    cursor = conn.cursor()

    # Check if the weather entry already exists
    cursor.execute("SELECT 1 FROM weather WHERE id = ?", (weather.activity_id,))
    existing_weather = cursor.fetchone()

    if not existing_weather:
        # Insert the weather data
        cursor.execute(
            """
            INSERT INTO weather (id, date, temp, weather_type, precipitation, rain, wind, snow)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                weather.activity_id,
                weather.date,
                weather.temp,
                weather.weather_type,
                weather.precipitation,
                weather.rain,
                weather.wind,
                weather.snow,
            ),
        )
        logger.info(
            f"Inserted new weather data for activity {weather.activity_id} into the database."
        )

    conn.commit()
    conn.close()

def insert_lap(lap):
    conn = connect_laps_db()
    cursor = conn.cursor()

    # Check if the lap already exists
    cursor.execute(
        "SELECT 1 FROM laps WHERE id = ? AND lap_index = ?",
        (lap.id, lap.lap_index),
    )
    existing_lap = cursor.fetchone()

    if not existing_lap:
        cursor.execute(
            """
            INSERT INTO laps (
                id, lap_name, duration, date, distance, average_speed, 
                lap_index, split, elevation_gain, average_heartrate, pace_zone
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lap.id,
                lap.lap_name,
                lap.duration,
                lap.date,
                lap.distance,
                lap.average_speed,
                lap.lap_index,
                lap.split,
                lap.elevation_gain,
                lap.average_heartrate,
                lap.pace_zone,
            ),
        )
        logger.info(f"Inserted new lap for activity {lap.id}, lap index {lap.lap_index}")

    conn.commit()
    conn.close()

def insert_split(row):
    """
    Insert a row into the splits table in splits.db only if it doesn't already exist.

    Args:
        row (pd.Series): A row from the DataFrame.
    """
    conn = connect_splits_db()
    cursor = conn.cursor()

    # Check if the split already exists
    cursor.execute("SELECT 1 FROM splits WHERE id = ?", (int(row["id"]),))
    existing_split = cursor.fetchone()

    if not existing_split:
        # Insert the split if it does not exist
        cursor.execute(
            """
            INSERT INTO splits (id, sport_type, splits_metric, laps, available_zones)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(row["id"]),  # Ensure id is an integer
                row["sport_type"],  # sport_type is already a string
                json.dumps(row["splits_metric"].tolist() if isinstance(row["splits_metric"], pd.Series) else row["splits_metric"]),
                json.dumps(row["laps"].tolist() if isinstance(row["laps"], pd.Series) else row["laps"]),
                json.dumps(row["available_zones"].tolist() if isinstance(row["available_zones"], pd.Series) else row["available_zones"]),
            )
        )
        logger.info(f"Inserted new split data for ID {row['id']} into the database.")
    else:
        logger.info(f"Split data for ID {row['id']} already exists in the database.")

    conn.commit()
    conn.close()

def insert_zone(row):
    """
    Insert a row into the zones table in zones.db only if it doesn't already exist.

    Args:
        row (pd.Series): A row from the DataFrame.
    """
    conn = connect_zones_db()
    cursor = conn.cursor()

    # Check if the zone already exists
    cursor.execute(
        "SELECT 1 FROM zones WHERE activity_id = ? AND zone_type = ? AND min_value = ? AND max_value = ?",
        (row["activity_id"], row["zone_type"], row["min_value"], row["max_value"]),
    )
    existing_zone = cursor.fetchone()

    if not existing_zone:
        # Insert the zone if it does not exist
        cursor.execute(
            """
            INSERT INTO zones (activity_id, zone_type, min_value, max_value, time_in_zone)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["activity_id"],
                row["zone_type"],
                row["min_value"],
                row["max_value"],
                row["time_in_zone"],
            ),
        )
        conn.commit()
        conn.close()
        logger.info(f"Inserted new zone data for Activity ID {row['activity_id']}")
        return True  # New data was inserted
    else:
        conn.close()
        return False  # Existing data, no insertion
