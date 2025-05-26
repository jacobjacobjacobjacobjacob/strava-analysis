# src/db/queries.py

ALLOWED_TABLES = [
    "activities",
    "best_efforts",
    "gear",
    "splits",
    "zones",
    "cache",
    "streams",
    "health",
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
    "health": """
    CREATE TABLE IF NOT EXISTS health (
        id,
        date TEXT PRIMARY KEY,
        apple_exercise_time REAL,
        apple_move_time REAL,
        caffeine REAL,
        cardio_recovery REAL,
        flights_climbed INTEGER,
        headphone_audio_exposure REAL,
        heart_rate_variability REAL,
        mindful_minutes REAL,
        physical_effort REAL,
        respiratory_rate REAL,
        resting_heart_rate REAL,
        running_ground_contact_time REAL,
        running_power REAL,
        running_speed REAL,
        running_stride_length REAL,
        running_vertical_oscillation REAL,
        sleep_analysis_asleep REAL,
        sleep_analysis_in_bed REAL,
        sleep_analysis_core REAL,
        sleep_analysis_deep REAL,
        sleep_analysis_rem REAL,
        sleep_analysis_awake REAL,
        step_count INTEGER,
        time_in_daylight REAL,
        vo2_max REAL,
        walking_running_distance REAL,
        month TEXT,
        day_of_week TEXT
    )
""",
}


INSERT_OR_REPLACE_QUERY = """
INSERT OR REPLACE INTO {table_name} ({columns})
VALUES ({placeholders})
"""

GET_CACHED_IDS = "SELECT id FROM cache;"
GET_DATES_FROM_HEALTH = "SELECT date FROM health;"

CLEAR_CACHE = "DELETE FROM cache;"

GET_ROW_COUNT = "SELECT COUNT(*) FROM {table_name}"

GET_ACTIVITIES_IDS = "SELECT id FROM activities;"
GET_ZONES_IDS = "SELECT id FROM zones;"
GET_SPLITS_IDS = "SELECT id FROM splits;"
GET_STREAMS_IDS = "SELECT id FROM streams;"
GET_BEST_EFFORTS_IDS = "SELECT id FROM best_efforts;"
GET_GEAR_IDS = "SELECT gear_id FROM gear;"

READ_TABLE_TO_DF = "SELECT * FROM {table_name};"


DELETE_LAST_ROW = """
    DELETE FROM {table_name}
    WHERE id = (SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1);
"""
