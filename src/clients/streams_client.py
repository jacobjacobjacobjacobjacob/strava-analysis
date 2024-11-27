# src/clients/streams.py
from loguru import logger
import pandas as pd
from src.utils import VALID_STREAM_TYPES


class StreamClient:

    def __init__(self, strava_client):
        self.strava_client = strava_client

    def _get_streams(self, activity_id, stream_types):
        """Private method to fetch multiple streams for a given activity."""
        VALID_STREAM_TYPES = [
            "time",
            "distance",
            "latlng",
            "altitude",
            "velocity_smooth",
            "heartrate",
            "cadence",
            "watts",
            "temp",
            "moving",
            "grade_smooth",
        ]
        for stream_type in stream_types:
            if stream_type not in VALID_STREAM_TYPES:
                logger.error(f"Invalid stream type requested: {stream_type}")
                raise ValueError(f"Invalid stream type requested: {stream_type}")

        stream_types_str = ",".join(stream_types)
        return self.strava_client.make_request(
            f"activities/{activity_id}/streams?keys={stream_types_str}&key_by_type=true"
        )

    def get_time_stream(self, activity_id):
        return self._get_streams(activity_id, ["time"])

    def get_distance_stream(self, activity_id):
        return self._get_streams(activity_id, ["distance"])

    def get_latlng_stream(self, activity_id):
        return self._get_streams(activity_id, ["latlng"])

    def get_altitude_stream(self, activity_id):
        return self._get_streams(activity_id, ["altitude"])

    def get_cadence_stream(self, activity_id):
        return self._get_streams(activity_id, ["cadence"])
    
    def get_pace_stream(self, activity_id):
        return self._get_streams(activity_id, ["pace"])

    def get_speed_stream(self, activity_id):
        return self._get_streams(activity_id, ["velocity_smooth"])

    def get_heartrate_stream(self, activity_id):
        return self._get_streams(activity_id, ["heartrate"])
    
    def get_activity_zones(self, activity_id):
        """Fetch heart rate and pace zones for a specific activity."""
        return self.strava_client.make_request(f"activities/{activity_id}/zones")

    @staticmethod
    def extract_stream_data(streams, stream_type):
        """Extract data from a specific stream type."""
        if not isinstance(streams, dict):
            logger.error("Expected streams to be a dictionary.")
            raise ValueError("Expected streams to be a dictionary.")

        stream = streams.get(stream_type)
        if stream and isinstance(stream, dict):
            return stream.get("data", [])
        return []  # Return empty list if stream_type not found
    
    @staticmethod
    def parse_activity_zones(zones):
        """Parse activity zones data into a structured format."""
        if not isinstance(zones, list):
            raise ValueError("Zones data should be a list.")

        parsed_zones = []
        for zone in zones:
            zone_type = zone.get("type")
            distribution_buckets = zone.get("distribution_buckets", [])
            for bucket in distribution_buckets:
                parsed_zones.append(
                    {
                        "zone_type": zone_type,
                        "min": bucket.get("min"),
                        "max": bucket.get("max"),
                        "time_in_zone": bucket.get("time"),
                    }
                )

        return pd.DataFrame(parsed_zones)

    def get_parsed_activity_zones(self, activity_id):
        """Fetch and parse activity zones for a given activity."""
        zones = self.get_activity_zones(activity_id)
        return self.parse_activity_zones(zones)
    
    def get_all_streams(self, activity_id):
        """Fetch all available streams for a specific activity."""
        logger.info(f"Fetching all streams for activity {activity_id}.")
        response = self._get_streams(activity_id, VALID_STREAM_TYPES)

        if not response:
            logger.error(f"No streams found for activity {activity_id}.")
            return {}

        # Extract data only for available streams
        available_streams = {
            stream_type: self.extract_stream_data(response, stream_type)
            for stream_type in VALID_STREAM_TYPES
            if stream_type in response
        }

        logger.info(f"Available streams for activity {activity_id}: {list(available_streams.keys())}")
        return available_streams
