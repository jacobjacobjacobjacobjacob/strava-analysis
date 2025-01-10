# src/queries.py

ALLOWED_TABLES = [
    "activities",
    "best_efforts",
    "gear",
    "weather",
    "splits",
    "zones",
    "cache",
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
                    temp REAL,
                    weather_type TEXT,
                    precipitation REAL,
                    rain REAL,
                    wind REAL,
                    snow REAL
                )
            """,
    "splits": """
                CREATE TABLE IF NOT EXISTS splits (
                    id INTEGER PRIMARY KEY,
                    sport_type TEXT,
                    splits_metric TEXT,
                    laps TEXT,
                    available_zones TEXT
                )
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
    "cache": """
                CREATE TABLE IF NOT EXISTS cache (
                    id INTEGER PRIMARY KEY
                )
            """,
}

GET_WEATHER_PARAMS = "SELECT date, lat_lng FROM activities WHERE id = ?;"

INSERT_OR_IGNORE_QUERY = (
    "INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
)

GET_CACHED_IDS = "SELECT id FROM cache;"

CLEAR_CACHE = "DELETE FROM cache;"

GET_ROW_COUNT = "SELECT COUNT(*) FROM {table_name}"

GET_ALL_ACTIVITY_IDS = "SELECT id FROM activities;"
GET_MISSING_ZONES = "SELECT id FROM zones;"
GET_MISSING_SPLITS = "SELECT id FROM splits;"