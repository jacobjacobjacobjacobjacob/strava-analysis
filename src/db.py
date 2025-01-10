# src/database/db.py
import sqlite3
import pandas as pd
from loguru import logger

from src.queries import (
    ALLOWED_TABLES,
    INSERT_ID_TO_CACHE,
    CREATE_ALL_TABLES,
    INSERT_OR_IGNORE_QUERY,
    GET_WEATHER_PARAMS,
    GET_CACHED_IDS,
    CLEAR_CACHE,
    GET_ROW_COUNT,
)
from src.config import DATABASE_PATH


class DatabaseManager:
    def __init__(self, db_path: str=DATABASE_PATH):
        """Initialize the DatabaseManager with a path to the database."""
        self.db_path = db_path

    def connect_db(self):
        """Connect to the SQLite database."""
        conn = sqlite3.connect(self.db_path)
        return conn

    def execute_query(self, query: str, params=None):
        """Execute a query on the database."""
        try:
            with self.connect_db() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params or ())
                conn.commit()
                logger.trace(f"Query executed:\n{query}\nParams:{params}")
                return cursor.fetchall()
            

        except sqlite3.Error as e:
            logger.error(f"Error executing query: {e}")
            return []
        
    def validate_table(self, table_name: str):
        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"Invalid table name: {table_name}")

    def create_table(self, create_table_query: str) -> None:
        """Creates a database table using the provided query."""
        self.execute_query(create_table_query)

    def create_all_tables(self) -> None:
        """Iterates through predefined table creation queries and creates all tables."""
        for table_name, create_query in CREATE_ALL_TABLES.items():
            try:
                self.create_table(create_query)
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")

    def update_cache(self, activity_id: int) -> None:
        """Updates the cache table by inserting or replacing an activity ID."""
        self.execute_query(INSERT_ID_TO_CACHE, (activity_id,))

    def get_ids_from_cache(self) -> list:
        """Fetches all IDs from the cache table."""
        return [row[0] for row in self.execute_query(GET_CACHED_IDS)]
    
    def get_ids_from_zones(self) -> list:
        """Fetches all IDs from the zones table."""
        return [row[0] for row in self.execute_query(GET_CACHED_IDS)]
    
    def get_ids_from_splits(self) -> list:
        """Fetches all IDs from the splits table."""
        return [row[0] for row in self.execute_query(GET_CACHED_IDS)]
    
    def get_ids_from_activities(self) -> list:
        """Fetches all IDs from the activities table."""
        return [row[0] for row in self.execute_query(GET_CACHED_IDS)]
    
    def check_discrepancies(self) -> None:
        zones_ids = self.get_ids_from_zones()
        splits_ids = self.get_ids_from_splits()
        activities_ids = self.get_ids_from_activities()  
        cached_ids = self.get_ids_from_cache()                  

        missing_zones = [item for item in activities_ids if item not in zones_ids]
        missing_splits = [item for item in activities_ids if item not in splits_ids]   
        missing_cache = [item for item in activities_ids if item not in cached_ids]    

        if len(missing_splits) != 0:
            logger.warning(f"{len(missing_splits)} activities are missing splits data.")
        if len(missing_zones) != 0:
            logger.warning(f"{len(missing_zones)} activities are missing zones data.")
        if len(missing_cache) != 0:
            logger.warning(f"{len(missing_cache)} activities are not present in cache.")
        else:
            logger.info("Zones, Splits and Cache is up to date.")

    
    def get_row_count(self, table_name: str) -> int:
        """Fetches the count of rows in the specified table"""
        row_count = self.execute_query(GET_ROW_COUNT.format(table_name=table_name))
        return row_count[0][0] if row_count else 0
    
    def clear_cache(self) -> None:
        """Clears all entries in the cache table."""
        logger.warning("ATTEMPTING TO CLEAR CACHE. ARE YOU SURE? (Y/N)")
        response = input()
        if response.upper() == "Y":
            try:
                self.execute_query(CLEAR_CACHE)
                logger.warning("Cache table cleared successfully.")
            except Exception as e:
                logger.error(f"Error clearing cache table: {e}")
        else:
            logger.warning("ABORTED. CACHE NOT CLEARED.")

    def get_weather_params_from_db(self, activity_id: int) -> tuple:
        """Fetches the weather-related parameters (date, lat_lng) for a given activity ID."""
        weather_params = self.execute_query(GET_WEATHER_PARAMS, (activity_id,))
        return weather_params[0] if weather_params else None

    def insert_dataframe_to_db(self, df: pd.DataFrame, table_name: str, query=INSERT_OR_IGNORE_QUERY, allowed_tables: list = ALLOWED_TABLES) -> None:
        """
        Inserts data from a Pandas DataFrame into a specified SQLite database table using predefined queries.

        Args:
            df (pd.DataFrame): A DataFrame containing the data to insert.
            table_name (str): The name of the table to insert data into.
            query (str): The predefined SQL query for inserting data.
        """
        # Validate table name
        self.validate_table(table_name)

        if df is None or df.empty:
            logger.warning(f"No data to insert into {table_name}. Skipping.")
            return

        # Prepare columns and placeholders
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['?' for _ in df.columns])

        # Format the query
        query = query.format(table_name=table_name, columns=columns, placeholders=placeholders)

        # Convert to list of tuples (records)
        data = df.to_dict(orient="records")


        try:
            for row in data:
                self.execute_query(query, tuple(row.values()))  # Insert each row
            logger.info(f"{len(df)} rows successfully inserted into the {table_name} table.")
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")

 