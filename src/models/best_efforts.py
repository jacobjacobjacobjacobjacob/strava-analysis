# src/models/best_efforts.py
import pandas as pd
import json
from loguru import logger


class BestEfforts:
    def __init__(
        self,
        id: int,
        date: str,
        name: str,
        distance: int,
        time: int,
        pr_rank: int,
    ):
        self.id: int = id
        self.date: str = date
        self.name: str = name
        self.distance: int = distance
        self.time: int = time
        self.pr_rank: int = pr_rank

    def __repr__(self):
        return (
            f"BestEffort(id={self.id}, date='{self.date}', name='{self.name}', "
            f"distance={self.distance}, time={self.time}, pr_rank={self.pr_rank})"
        )
    
    @staticmethod
    def calculate_kph(distance_meters, time_seconds):
        """Calculate the speed in kilometers per hour."""
        if time_seconds == 0:  # Avoid division by zero
            return 0
        return (distance_meters / 1000) / (time_seconds / 3600)

    @staticmethod
    def format_kph_to_pace(kph):
        """Convert speed (kph) to pace (time per km)."""
        if kph == 0:
            return "N/A"
        pace_minutes = 60 / kph
        pace_seconds = (pace_minutes - int(pace_minutes)) * 60
        return f"{int(pace_minutes)}:{int(pace_seconds):02d} min/km"
    
    @staticmethod
    def convert_seconds_to_hms(time_seconds):
        """Convert seconds to HH:MM:SS format."""
        hours, remainder = divmod(time_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
    
    @staticmethod
    def check_new_personal_bests(df: pd.DataFrame) -> None:
        if df.empty:
            return
        # Filter rows where `pr_rank` is 1
        new_personal_bests = df[df["pr_rank"] == 1]

        # Iterate through each personal best
        for _, row in new_personal_bests.iterrows():
            distance = row["name"]
            time_seconds = row["time"]

            # Calculate time and pace
            time_hms = BestEfforts.convert_seconds_to_hms(time_seconds)
            kph = BestEfforts.calculate_kph(row["distance"], time_seconds)
            pace = BestEfforts.format_kph_to_pace(kph)

            # Log the success message
            logger.success(
                f"New Personal Best on the {distance} - {time_hms} @ {pace})!"
            )

    @staticmethod
    def process_best_efforts(activity_id: int, best_efforts_list: list) -> pd.DataFrame:
        best_efforts_data = []  # List to hold the best efforts data

        best_efforts = json.dumps(best_efforts_list)

        best_efforts_data.append([activity_id, best_efforts])

        best_efforts = [
            {
                "id": effort["activity"]["id"],
                "date": effort["start_date_local"][:10],  # Extract YYYY-MM-DD
                "name": effort["name"],
                "distance": effort["distance"],
                "time": effort["moving_time"],
                "pr_rank": (
                    effort["pr_rank"] if effort["pr_rank"] is not None else 0
                ),  # Convert None to 0
            }
            for effort in best_efforts_list
        ]

        # Convert the list of dictionaries to a DataFrame
        best_efforts_df = pd.DataFrame(best_efforts)

        # Check for new best efforts
        BestEfforts.check_new_personal_bests(best_efforts_df)
  
        return best_efforts_df
