# app/app_config.py
# User settings for the Streamlit Dashboard
from src.analysis.constants import CURRENT_YEAR
class Config:
        
    SPORT_TYPES = [
        "Run",
        "Ride",
        "Walk"]

    DEFAULT_SPORT_TYPE = "Run"
    DEFAULT_YEAR = CURRENT_YEAR

