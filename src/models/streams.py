# src/models/streams.py
from loguru import logger
import pandas as pd
class Streams:
    def __init__(self, activity_id, streams):
        self.activity_id = activity_id
        self.streams = streams

    def __repr__(self):
        return f"Streams(activity_id={self.activity_id}, streams='{self.streams}')"

    def save_to_db(self):
        """Save this stream instance to the database."""
        from src.database.db import insert_stream
        insert_stream(self)
        
    @staticmethod
    def parse_streams_to_dataframe_compact(activity_id, streams_response):
        """
        Parses the streams response and structures it into a single-row DataFrame 
        with each stream's data wrapped into a list.
        
        :param activity_id: The ID of the activity.
        :param streams_response: The API response containing stream data.
        :return: A single-row DataFrame.
        """
        # Initialize the parsed data dictionary
        parsed_data = {"activity_id": int(activity_id)}

        # Process each stream
        for stream_type, stream_data in streams_response.items():
            # Wrap data in a list, or set to None if stream data is missing
            if isinstance(stream_data, list):
                parsed_data[stream_type] = stream_data
            else:
                parsed_data[stream_type] = None

        # Convert the dictionary into a single-row DataFrame
        return pd.DataFrame([parsed_data])
