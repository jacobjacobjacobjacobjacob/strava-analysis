# src/client/streams.py
from src.assets.constants import VALID_STREAM_TYPES
from loguru import logger


class StreamClient:
    def __init__(self, strava_client):
        self.strava_client = strava_client

    def _get_streams(self, activity_id, stream_types):
        """Private method to fetch multiple streams for a given activity."""
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

    def get_speed_stream(self, activity_id):
        return self._get_streams(activity_id, ["velocity_smooth"])

    def get_heartrate_stream(self, activity_id):
        return self._get_streams(activity_id, ["heartrate"])

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
