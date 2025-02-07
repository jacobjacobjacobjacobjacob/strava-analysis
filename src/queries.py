# src/queries.py

ALLOWED_TABLES = [
    "activities",
    "best_efforts",
    "gear",
    "weather",
    "splits",
    "zones",
    "cache",
    "streams",
]
INSERT_ID_TO_CACHE = """
        INSERT OR REPLACE INTO cache (id)
        VALUES (?)
    """


CREATE_ALL_TABLES = {
    "activities": """
                CREATE TABLE IF NOT EXISTS activities (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
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
            """,
    "best_efforts": """
                CREATE TABLE IF NOT EXISTS best_efforts (
                    id TEXT,
                    date TEXT,
                    name TEXT,
                    distance REAL,
                    time INTEGER,
                    pr_rank INTEGER,
                    PRIMARY KEY (id, name)
                )
            """,
    "gear": """
                CREATE TABLE IF NOT EXISTS gear (
                    gear_id TEXT PRIMARY KEY,
                    name TEXT,
                    distance REAL,
                    brand_name TEXT,
                    model_name TEXT,
                    retired INTEGER,
                    weight REAL
                )
            """,
    "weather": """
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY,
                    date TEXT,
                    temperature REAL,
                    weather_code TEXT,
                    precipitation REAL,
                    rain REAL,
                    wind_speed REAL,
                    snow REAL
                )
            """,
    "splits": """
                CREATE TABLE IF NOT EXISTS splits (
                    id INTEGER PRIMARY KEY,
                    sport_type TEXT,
                    splits_metric TEXT,
                    laps TEXT,
                    available_zones TEXT)
                
            """,
    "zones": """
                CREATE TABLE IF NOT EXISTS zones (
                    id INTEGER,
                    zone_type TEXT,
                    min_value INTEGER,
                    max_value INTEGER,
                    time_in_zone REAL,
                    PRIMARY KEY (id, zone_type, min_value, max_value)
                )
            """,
    "streams": """
                CREATE TABLE IF NOT EXISTS streams (
                    id INTEGER PRIMARY KEY,
                    time TEXT,
                    distance TEXT,
                    latlng TEXT,
                    altitude TEXT,
                    speed TEXT,
                    heartrate TEXT,
                    cadence TEXT,
                    watts TEXT
                )
            """,
    "cache": """
                CREATE TABLE IF NOT EXISTS cache (
                    id INTEGER PRIMARY KEY
                )
            """,
}

GET_WEATHER_PARAMS = (
    "SELECT id, date, start_time, lat_lng FROM activities WHERE id = ?;"
)

ADD_WEATHER_DATA = """
UPDATE activities
SET temperature = ?, wind_speed = ?, snow = ?
WHERE id = ?;
"""

INSERT_OR_IGNORE_QUERY = (
    "INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
)


GET_CACHED_IDS = "SELECT id FROM cache;"

CLEAR_CACHE = "DELETE FROM cache;"

GET_ROW_COUNT = "SELECT COUNT(*) FROM {table_name}"

GET_ACTIVITIES_IDS = "SELECT id FROM activities;"
GET_ZONES_IDS = "SELECT id FROM zones;"
GET_SPLITS_IDS = "SELECT id FROM splits;"
GET_STREAMS_IDS = "SELECT id FROM streams;"
GET_BEST_EFFORTS_IDS = "SELECT id FROM best_efforts;"
