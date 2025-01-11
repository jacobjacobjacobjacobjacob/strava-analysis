import pandas as pd
from loguru import logger
from src.constants import DEFAULT_COORDINATES


class Activity:
    def __init__(
        self,
        id,
        name,
        date,
        month,
        day_of_week,
        start_time,
        end_time,
        sport_type,
        indoor,
        distance,
        duration,
        elevation_gain,
        gear_id,
        average_heartrate,
        average_speed,
        average_cadence,
        average_temp,
        average_watts,
        intensity,
        lat_lng,
    ):
        self.id = id
        self.name = name
        self.date = date
        self.month = month
        self.day_of_week = day_of_week
        self.start_time = start_time
        self.end_time = end_time
        self.sport_type = sport_type
        self.indoor = indoor
        self.distance = distance
        self.duration = duration
        self.elevation_gain = elevation_gain
        self.gear_id = gear_id
        self.average_heartrate = average_heartrate
        self.average_speed = average_speed
        self.average_cadence = average_cadence
        self.average_temp = average_temp
        self.average_watts = average_watts
        self.intensity = intensity
        self.lat_lng = lat_lng

    def __repr__(self):
        return (
            f"Activity(activity_id={self.id}, name='{self.name}', date='{self.date}', month='{self.month}', "
            f"day_of_week='{self.day_of_week}', start_time='{self.start_time}', end_time='{self.end_time}', "
            f"sport_type='{self.sport_type}', indoor={self.indoor}, distance={self.distance}, "
            f"duration={self.duration}, elevation_gain={self.elevation_gain}, gear_id='{self.gear_id}', "
            f"average_heartrate={self.average_heartrate}, average_speed={self.average_speed}, "
            f"average_cadence={self.average_cadence}, average_temp={self.average_temp}, "
            f"average_watts={self.average_watts}, intensity={self.intensity}, lat_lng={self.lat_lng})"
        )

    @staticmethod
    def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Renames columns for clarity."""
        return df.rename(
            columns={
                "moving_time": "duration",
                "total_elevation_gain": "elevation_gain",
                "start_date_local": "date",
                "trainer": "indoor",
                "suffer_score": "intensity",
                "start_latlng": "lat_lng",
            }
        )

    @staticmethod
    def convert_units(df: pd.DataFrame) -> pd.DataFrame:
        """Converts units for specific columns."""
        unit_conversions = {
            "distance": lambda m: m / 1000,  # meters to kilometers
            "duration": lambda sec: sec / 60,  # seconds to minutes
            "average_speed": lambda mps: mps * 3.6,  # m/s to km/h
        }
        for col, conversion in unit_conversions.items():
            if col in df:
                df[col] = df[col].apply(conversion)
        return df

    @staticmethod
    def split_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Splits the 'date' column into separate date components and times."""
        if "date" not in df or "duration" not in df:
            raise ValueError('Missing required columns: "date" and "duration".')

        df["start_time"] = pd.to_datetime(df["date"])
        df["date"] = df["start_time"].dt.strftime("%Y-%m-%d")
        df["month"] = df["start_time"].dt.strftime("%m")
        df["day_of_week"] = df["start_time"].dt.strftime("%A").str.title()
        df["end_time"] = df["start_time"] + pd.to_timedelta(df["duration"], unit="m")

        df["start_time"] = df["start_time"].dt.strftime("%H:%M")
        df["end_time"] = df["end_time"].dt.strftime("%H:%M")

        return df

    @staticmethod
    def replace_lat_lng_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts lat_lng column lists to comma-separated strings, replaces "0, 0" with a valid default value.
        """
        df["lat_lng"] = df["lat_lng"].apply(
            lambda x: (
                DEFAULT_COORDINATES
                if (isinstance(x, list) and len(x) == 0) or x == "0, 0"
                else ", ".join(map(str, x)) if isinstance(x, list) else x
            )
        )
        return df
    
    @staticmethod
    def filter_sport_types(df: pd.DataFrame, sport_types: list = ["Ride", "Run"]) -> pd.DataFrame:
        logger.info(f"Filtered data by {sport_types}")
        return df[df["sport_type"].isin(sport_types)]
        

    @staticmethod
    def process_activity_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes raw activity data and caches new activity IDs.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame.")
        

        if df.empty:
            raise ValueError("DataFrame is empty.")

        try:
            processed_df = (
                df.pipe(Activity.rename_columns)
                .pipe(Activity.filter_sport_types)
                .pipe(Activity.convert_units)
                .pipe(Activity.split_datetime_columns)
                .pipe(Activity.replace_lat_lng_values)
            )
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise

        df = processed_df[
            [
                "id",
                "name",
                "date",
                "month",
                "day_of_week",
                "start_time",
                "end_time",
                "sport_type",
                "indoor",
                "distance",
                "duration",
                "elevation_gain",
                "gear_id",
                "average_heartrate",
                "average_speed",
                "average_cadence",
                "average_temp",
                "average_watts",
                "intensity",
                "lat_lng",
            ]
        ]

        return df
