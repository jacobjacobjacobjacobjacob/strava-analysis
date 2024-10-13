# utils.py
import pandas as pd


def map_months(df: pd.DataFrame):
    df["month"] = df["month"].map(MONTH_MAPPING)
    return df


MONTH_MAPPING = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec",
}

MONTH_ORDER = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]

WEEKDAY_MAPPING = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}

WEEKDAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

WEATHER_CODE_MAPPING = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Cloudy",
    4: "Overcast",
    45: "Fog at a distance",
    5: "Fog",
    6: "Drizzle",
    7: "Rain",
    8: "Showers",
    9: "Thunderstorms",
    10: "Snow",
    11: "Snow showers",
}
