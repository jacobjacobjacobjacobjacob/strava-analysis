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
    GET_STREAMS_IDS,
    GET_ACTIVITIES_IDS,
    GET_SPLITS_IDS,
    GET_ZONES_IDS,
    GET_BEST_EFFORTS_IDS,
    GET_ROW_COUNT,
    ADD_WEATHER_DATA,
)
from src.config import DATABASE_PATH


class DatabaseManager:
    def __init__(self, db_path: str = DATABASE_PATH):
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

    def get_ids_from_streams(self) -> list:
        """Fetches all IDs from the cache table."""
        return [row[0] for row in self.execute_query(GET_STREAMS_IDS)]

    def get_ids_from_splits(self) -> list:
        """Fetches all IDs from the splits table."""
        return [row[0] for row in self.execute_query(GET_SPLITS_IDS)]

    def get_ids_from_zones(self) -> list:
        """Fetches all IDs from the zones table."""
        return [row[0] for row in self.execute_query(GET_ZONES_IDS)]

    def get_ids_from_best_efforts(self) -> list:
        """Fetches all IDs from the best efforts table."""
        return [row[0] for row in self.execute_query(GET_BEST_EFFORTS_IDS)]

    def get_ids_from_activities(self) -> list:
        """Fetches all IDs from the activities table."""
        return [row[0] for row in self.execute_query(GET_ACTIVITIES_IDS)]

    def check_discrepancies(self) -> None:
        activities_ids = self.get_ids_from_activities()
        cached_ids = self.get_ids_from_cache()
        missing_cache = [item for item in activities_ids if item not in cached_ids]

        if len(missing_cache) != 0:
            logger.warning(f"{len(missing_cache)} activities are not present in cache.")

        else:
            return

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

    def insert_dataframe_to_db(
        self,
        df: pd.DataFrame,
        table_name: str,
        query=INSERT_OR_IGNORE_QUERY,
        allowed_tables: list = ALLOWED_TABLES,
    ) -> None:
        """
        Inserts data from a Pandas DataFrame into a specified SQLite database table using predefined queries.

        Args:
            df (pd.DataFrame): A DataFrame containing the data to insert.
            table_name (str): The name of the table to insert data into.
            query (str): The predefined SQL query for inserting data.
        """
        # Validate table name
        self.validate_table(table_name)
        # logger.debug(f"Inserting to table: {table_name}")

        if df is None or df.empty:
            # logger.warning(f"No data to insert into table: {table_name}. Skipping.")
            return

        # Prepare columns and placeholders
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["?" for _ in df.columns])

        # Format the query
        query = query.format(
            table_name=table_name, columns=columns, placeholders=placeholders
        )


        # Convert to list of tuples (records)
        data = df.to_dict(orient="records")

        try:
            for row in data:
                self.execute_query(query, tuple(row.values()))  # Insert each row

            if len(df) == 1:
                rows_string = "row"
            else:
                rows_string = "rows"
            # logger.info(
            #     f"Inserted {len(df)} {rows_string} into the {table_name} table."
            # )
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")

    def add_weather_data(
        self, activity_id: int, df: pd.DataFrame, query=ADD_WEATHER_DATA
    ) -> None:
        """Updates weather data for a specific activity in the database."""
        if df is None or df.empty:
            logger.warning(f"No weather data for activity: {activity_id}. Skipping.")
            return

        weather_data = df.iloc[0]
        logger.debug("WEATHER DATA RECEIVED:")
        print(weather_data)

        # Extract weather data from the dataframe
        temperature = weather_data["temperature"]
        wind_speed = weather_data["wind_speed"]
        snow = weather_data["snow"]
        weather_code = weather_data["weather_code"]
        rain = weather_data["rain"]
        precipitation = weather_data["precipitation"]

        logger.debug(f"Snow:{snow}, Wind:{wind_speed}, Temperature:{temperature}")

        # Execute the query to update the weather data in the activities table
        self.execute_query(
            query,
            (
                temperature,
                wind_speed,
                snow,
                weather_code,
                rain,
                precipitation,
                activity_id,
            ),
        )
        logger.info(f"Weather data updated for activity ID: {activity_id}")
