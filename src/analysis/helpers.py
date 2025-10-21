# src/analysis/helpers.py
import pandas as pd
from loguru import logger
from rich.console import Console
from rich.table import Table


console = Console()


def print_rich_dataframe(
    df: pd.DataFrame,
    title: str = "DataFrame",
    highlight_columns=None,
    max_rows: int = 20,
):
    """Prints a Pandas DataFrame as a styled Rich table.

    Args:
        df (pd.DataFrame): The DataFrame to print.
        title (str): Table title.
        highlight_columns (list): Columns to highlight in bold.
        max_rows (int): Maximum number of rows to display.
    """
    if df.empty:
        console.print(f"[bold red]{title} is empty![/bold red]")
        return

    table = Table(title=title)

    # Highlight specific columns if provided
    highlight_columns = set(highlight_columns or [])

    # Add column headers
    for col in df.columns:
        style = "bold yellow" if col in highlight_columns else "cyan"
        table.add_column(str(col), justify="right", style=style)

    # Add rows, handling NaNs and limiting output
    for _, row in df.head(max_rows).iterrows():
        table.add_row(*[str(x) if pd.notna(x) else "-" for x in row])

    if len(df) > max_rows:
        console.print(
            f"[bold yellow]Showing first {max_rows} of {len(df)} rows...[/bold yellow]"
        )

    console.print(table)


def get_total_by_metric(df: pd.DataFrame, metric: str, sport_type: str) -> int:
    """Calculate the total distance, duration and elevation for a given sport type"""

    # Filter by sport type
    df = df[df["sport_type"] == sport_type]
    # Filter by metric
    if metric == "distance":
        total_metric = df["distance"].sum()
    elif metric == "duration":
        total_metric = df["duration"].sum() / 60
    elif metric == "elevation_gain":
        total_metric = df["elevation_gain"].sum()

    else:
        raise ValueError(f"Invalid metric: {metric}")

    total_metric = int(total_metric)
    return total_metric


def get_monthly_cumsum(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    # Ensure data is sorted by date
    df = df.sort_values(by="date")

    # Aggregate metric per month
    if metric in df.columns:
        monthly_df = df.groupby("month", observed=True, sort=False)[metric].sum().reset_index()

        # Compute cumulative sum across months
        monthly_df[f"cumulative_{metric}"] = monthly_df[metric].cumsum()
    else:
        raise ValueError(f"Invalid metric: {metric}")
    
    return monthly_df

def format_kph_to_pace(kph):
        """Convert speed (kph) to pace (time per km)."""
        if kph == 0:
            return "N/A"
        pace_minutes = 60 / kph
        pace_seconds = (pace_minutes - int(pace_minutes)) * 60
        return f"{int(pace_minutes)}:{int(pace_seconds):02d} min/km"