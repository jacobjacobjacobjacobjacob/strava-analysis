# src/models/streams.py
import pandas as pd
import json
from loguru import logger
from src.constants import ALL_STREAM_TYPES

"""
FETCH THE STREAM FROM THE ACTIVITY
VERIFY THAT THE STREAMS EXIST

CREATE DATABASE TABLE
columns:
- activity id
- stream type
- list with values
"""


class Streams:
    def __init__(self, strava_client):
        self.strava_client = strava_client

    @staticmethod
    def process_streams(activity_id, response) -> pd.DataFrame:
        """Process and serialize streams data."""
        streams_data = []

        keys = [
            "time",
            "distance",
            "latlng",
            "altitude",
            "velocity_smooth",
            "heartrate",
            "cadence",
            "watts",
        ]

        # Initialize a dictionary to hold stream data for the activity
        row_data = {"id": activity_id}

        for key in keys:
            stream_values = response.get(key, {}).get("data", None)

            # Serialize the list as JSON if it exists
            row_data[key] = (
                json.dumps(stream_values) if stream_values else json.dumps([0])
            )

        # Rename `velocity_smooth` to `speed`
        row_data["speed"] = row_data.pop("velocity_smooth", json.dumps([0]))

        streams_data.append(row_data)

   
        columns = [
            "id",
            "time",
            "distance",
            "latlng",
            "altitude",
            "speed",
            "heartrate",
            "cadence",
            "watts",
        ]
        streams_df = pd.DataFrame(streams_data, columns=columns)

        return streams_df

    def get_streams(
        self, activity_id, keys=ALL_STREAM_TYPES, resolution="low", key_by_type=True
    ) -> pd.DataFrame:
        """Fetches streams from the activity."""
        params = {
            "keys": (
                ",".join(keys) if keys else None
            ),  # Join keys into a comma-separated string
            "resolution": resolution,
            "key_by_type": str(key_by_type).lower(),
        }

        try:
            streams_response = self.strava_client.make_request(
                f"activities/{activity_id}/streams", params=params
            )

            if not streams_response:  # Check if the response is None or empty
                logger.info(
                    f"No stream data found for activity {activity_id}, skipping."
                )
                return (
                    pd.DataFrame()
                )  # Return an empty DataFrame if no streams data is found

            streams_df = self.process_streams(activity_id, streams_response)
            return streams_df

        except Exception as e:
            logger.error(f"Error fetching streams for activity {activity_id}: {e}")
            return pd.DataFrame() 
