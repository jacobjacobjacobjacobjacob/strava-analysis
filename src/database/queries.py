# src/database/queries.py
import sqlite3
import pandas as pd


def get_table_data(table_name: str, db_name: str):
    """
    Retrieves all the data from a given table as a Pandas DataFrame.

    :param table_name: Name of the table to query.
    :param db_path: Path to the SQLite database file.
    :return: DataFrame containing the table data.
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(f"data/db_name")

        # Query to get all data from the specified table
        query = f"SELECT * FROM {table_name}"

        # Load the data into a DataFrame
        df = pd.read_sql_query(query, conn)

        return df
    except sqlite3.Error as e:
        print(f"Error retrieving table data: {e}")
        return None
    finally:
        if conn:
            conn.close()


# Query to join the databses for later:

# SELECT a.*, w.temperature, w.precipitation

# FROM activities a
# JOIN weather w ON a.activity_id = w.activity_id;
