# src/config.py
import os
from dotenv import load_dotenv

load_dotenv()
def get_strava_config():
    strava_config = {
        "client_id": os.getenv("STRAVA_CLIENT_ID"),
        "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
        "refresh_token": os.getenv("STRAVA_REFRESH_TOKEN"),
        "athlete_id": os.getenv("STRAVA_ATHLETE_ID"),
    }
    return strava_config


DATABASE_NAME = "database.db"
DATABASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", DATABASE_NAME)
