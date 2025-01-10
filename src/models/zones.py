# src/models/zones.py
import pandas as pd
from loguru import logger
# from src.db import insert_data_to_db


class Zones:
    def __init__(self, zone_data):
        self.splits_metric = zone_data

    def __repr__(self):
        return f"Zones(zone_data={self.zone_data}"

    @staticmethod
    def process_zones(zone_data: list, activity_id: int) -> pd.DataFrame:
        if not isinstance(zone_data, list):
            raise ValueError("Zones data should be a list.")

        parsed_zones = []
        for zone in zone_data:
            zone_type = zone.get("type", "unknown")
            distribution_buckets = zone.get("distribution_buckets", [])
            if not isinstance(distribution_buckets, list):
                logger.warning(
                    f"Invalid distribution_buckets for zone_type {zone_type}, skipping."
                )
                continue
            for bucket in distribution_buckets:
                parsed_zones.append(
                    {
                        "id": activity_id,
                        "zone_type": zone_type,
                        "min_value": bucket.get("min"),
                        "max_value": bucket.get("max"),
                        "time_in_zone": bucket.get("time"),
                    }
                )
        zones_df = pd.DataFrame(parsed_zones)

       

        return zones_df


