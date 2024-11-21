# src/models/zones.py
from loguru import logger

class Zones:
    def __init__(
        self,
        activity_id,
        zone_type,
        min_value,
        max_value,
        time_in_zone,
    ):
        self.activity_id = activity_id
        self.zone_type = zone_type
        self.min_value = min_value
        self.max_value = max_value
        self.time_in_zone = time_in_zone

    def __repr__(self):
        return (
            f"Zone(activity_id={self.activity_id}, zone_type='{self.zone_type}', "
            f"min_value={self.min_value}, max_value={self.max_value}, "
            f"time_in_zone={self.time_in_zone})"
        )

    def save_to_db(self):
        """Save the zone to the database."""
        from src.database.db import insert_zone

        return insert_zone(self.to_series())

    def to_series(self):
        """Convert the Zone instance to a pandas Series for compatibility with insert_zone."""
        import pandas as pd

        return pd.Series(
            {
                "activity_id": self.activity_id,
                "zone_type": self.zone_type,
                "min_value": self.min_value,
                "max_value": self.max_value,
                "time_in_zone": self.time_in_zone,
            }
        )

    @classmethod
    def process_zones(cls, zones_df):
        """Process and save zones from a DataFrame to the database."""
        new_zone_ids = []

        for _, row in zones_df.iterrows():
            # Create a Zone instance from the row
            zone = cls(
                activity_id=row["activity_id"],
                zone_type=row["zone_type"],
                min_value=row["min"],
                max_value=row["max"],
                time_in_zone=row["time_in_zone"],
            )
            # Save the Zone instance to the database
            if zone.save_to_db():
                new_zone_ids.append((zone.activity_id, zone.zone_type))

        # Log a summary after all zones are processed
        if new_zone_ids:
            logger.info(
                f"Processed and saved {len(new_zone_ids)} zones for Activity ID {zones_df['activity_id'].iloc[0]}"
            )
        else:
            logger.info(f"No new zones were added for Activity ID {zones_df['activity_id'].iloc[0]}")

        return new_zone_ids
    
    @classmethod
    def parse_activity_zones(cls, zones, activity_id):
        """
        Parse activity zones data into a structured DataFrame.

        Parameters:
        zones (list): The raw zones data from the API.

        Returns:
        pd.DataFrame: Parsed zone data.
        """
        import pandas as pd

        if not isinstance(zones, list):
            raise ValueError("Zones data should be a list.")

        parsed_zones = []
        for zone in zones:
            zone_type = zone.get("type", "unknown")
            distribution_buckets = zone.get("distribution_buckets", [])
            if not isinstance(distribution_buckets, list):
                print(f"Invalid distribution_buckets for zone_type {zone_type}, skipping.")
                continue
            for bucket in distribution_buckets:
                parsed_zones.append(
                    {
                        "activity_id": activity_id, 
                        "zone_type": zone_type,
                        "min": bucket.get("min"),
                        "max": bucket.get("max"),
                        "time_in_zone": bucket.get("time"),
                    }
                )

        return pd.DataFrame(parsed_zones)
