import sqlite3
import pandas as pd


def get_table_data(table_name: str, db_name="activities.db"):
    """
    Retrieves all the data from a given table and prints it.
    """
    conn = sqlite3.connect(f"data/{db_name}")
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    conn.close()

    return df
