# app/utils.py
import pandas as pd
import streamlit as st
from src.db.db_manager import DatabaseManager
from app.app_config import Config


def format_kph_to_pace(kph):
    if kph == 0:
        return "N/A"
    pace_minutes = 60 / kph
    pace_seconds = (pace_minutes - int(pace_minutes)) * 60
    return f"{int(pace_minutes)}:{int(pace_seconds):02d}"


def format_date_columns(df):
    # Convert date and extract month/year properly
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.strftime("%b")
    df["year"] = df["date"].dt.year
    return df


def filter_sport_types(df, sport_types=Config.SPORT_TYPES):
    if sport_types:
        df = df[df["sport_type"].isin(sport_types)]
    return df


@st.cache_data
def load_data(table_name: str = "activities"):
    db_manager = DatabaseManager()
    df = db_manager.get_table_as_dataframe(table_name=table_name)
    df.copy()

    # Formatting and filtering
    df = format_date_columns(df)
    df = filter_sport_types(df)

    return df
