# src/models/split.py
import pandas as pd
import json
from loguru import logger



class Splits:
    def __init__(
        self,
        split_id,
        sport_type,
        splits_metric,
        laps,
        available_zones,
    ):
        self.split_id = split_id
        self.sport_type = sport_type
        self.splits_metric = splits_metric
        self.laps = laps
        self.available_zones = available_zones

    def __repr__(self):
        return (
            f"Split(split_id={self.split_id}, sport_type='{self.sport_type}', "
            f"splits_metric={self.splits_metric}, laps={self.laps}, "
            f"available_zones={self.available_zones})"
        )

    @staticmethod
    def process_splits(client, df: pd.DataFrame):
        """Fetch splits details"""
        splits_data = []

        for _, row in df.iterrows():
            activity_id = row["id"]
            sport_type = row["sport_type"]

            # Safely access 'splits_metric' or skip if missing
            splits_metric = row.get("splits_metric", None)
            if splits_metric is None:
                continue

            laps = row.get("laps", None)
            available_zones = row.get("available_zones", None)

            # Serialize lists or dicts to JSON
            splits_metric_json = json.dumps(splits_metric) if isinstance(splits_metric, (list, dict)) else splits_metric
            laps_json = json.dumps(laps) if isinstance(laps, (list, dict)) else laps
            available_zones_json = json.dumps(available_zones) if isinstance(available_zones, (list, dict)) else available_zones

            splits_data.append([
                activity_id, 
                sport_type, 
                splits_metric_json, 
                laps_json, 
                available_zones_json
            ])

        splits_df = pd.DataFrame(
            splits_data,
            columns=["id", "sport_type", "splits_metric", "laps", "available_zones"]
        )

        return splits_df

        