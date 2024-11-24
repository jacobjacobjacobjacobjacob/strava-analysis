# src/models/streams.py
from loguru import logger

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

