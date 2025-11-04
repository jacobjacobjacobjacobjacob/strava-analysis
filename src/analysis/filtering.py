# src/analysis/filtering.py
from typing import Optional, Union, List
from loguru import logger
import pandas as pd
from datetime import datetime

from src.analysis.constants import ALL_MONTHS


class FilteringError(Exception):
    """Custom exception for filtering operations"""
    pass


def _validate_dataframe(df: pd.DataFrame) -> None:
    """Validate input DataFrame structure"""
    if not isinstance(df, pd.DataFrame):
        raise FilteringError("Input must be a pandas DataFrame")
    if df.empty:
        raise FilteringError("Cannot filter empty DataFrame")


def filter_by_sport_type(df: pd.DataFrame, sport_type: str) -> pd.DataFrame:
    """Filter DataFrame by specific sport type

    Args:
        df: Input DataFrame
        sport_type: Sport type to filter by

    Returns:
        Filtered DataFrame

    Raises:
        FilteringError: If sport_type column is missing or filtering fails
    """
    try:
        _validate_dataframe(df)

        if "sport_type" not in df.columns:
            raise FilteringError("Column 'sport_type' not found in DataFrame")

        filtered = df[df["sport_type"] == sport_type].copy()
        # logger.info(f"Filtered on sport_type={sport_type} | Shape: {filtered.shape}")
        return filtered

    except Exception as e:
        logger.error(f"Failed to filter by sport_type: {str(e)}")
        raise FilteringError(f"Sport type filtering failed: {str(e)}")


def filter_by_time_period(
    df: pd.DataFrame,
    years: Optional[List[int]] = None,
    months: Optional[List[str]] = None,
    weekdays: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Filter DataFrame by time period (years, months, weekdays)

    Args:
        df: Input DataFrame
        years: List of years to include
        months: List of month abbreviations to include
        weekdays: List of weekday abbreviations to include

    Returns:
        Filtered DataFrame

    Raises:
        FilteringError: If required columns are missing or date conversion fails
    """
    try:
        _validate_dataframe(df)
        filtered = df.copy()

        # Validate date column exists
        if "date" not in filtered.columns:
            raise FilteringError("Column 'date' not found for time period filtering")

        # Year filtering - handle directly from string
        if years:
            # Extract first 4 characters as year
            filtered.loc[:, "year_str"] = filtered["date"].str[:4]

            # Convert to integer for comparison
            try:
                filtered.loc[:, "year"] = filtered["year_str"].astype(int)
            except ValueError as e:
                raise FilteringError(
                    f"Could not convert year string to integer: {str(e)}"
                )

            available_years = filtered["year"].dropna().unique()
            invalid_years = set(years) - set(available_years)

            if invalid_years:
                logger.warning(f"Years not found in data: {invalid_years}")
                years = [y for y in years if y in available_years]

            if years:
                filtered = filtered[filtered["year"].isin(years)]
                # logger.info(f"Filtered on years: {years} | Shape: {filtered.shape}")

        # For month and weekday filtering, we still need datetime conversion
        if months or weekdays:
            try:
                # First check if already datetime
                if not pd.api.types.is_datetime64_any_dtype(filtered["date"]):
                    # Try parsing with dayfirst=False (MM-DD-YYYY)
                    filtered.loc[:, "date"] = pd.to_datetime(
                        filtered["date"], format="%Y-%m-%d", errors="raise"
                    )

                # Verify we have valid datetimes
                if filtered["date"].isna().any():
                    invalid_dates = filtered[filtered["date"].isna()]["date"].unique()
                    raise FilteringError(
                        f"Could not convert dates to datetime: {invalid_dates}"
                    )

            except Exception as e:
                raise FilteringError(f"Date conversion failed: {str(e)}")

            # Month filtering
            if months:
                filtered.loc[:, "month_abbr"] = filtered["date"].dt.strftime("%b")
                invalid_months = set(months) - set(ALL_MONTHS)
                if invalid_months:
                    logger.warning(f"Invalid month abbreviations: {invalid_months}")
                    months = [m for m in months if m in ALL_MONTHS]

                if months:
                    filtered = filtered[filtered["month_abbr"].isin(months)]
                    # logger.info(
                    #     f"Filtered on months: {months} | Shape: {filtered.shape}"
                    # )

            # Weekday filtering
            if weekdays:
                filtered.loc[:, "day_of_week_abbr"] = filtered["date"].dt.strftime("%a")
                available_weekdays = filtered["day_of_week_abbr"].dropna().unique()
                invalid_weekdays = set(weekdays) - set(available_weekdays)

                if invalid_weekdays:
                    logger.warning(f"Weekdays not found in data: {invalid_weekdays}")
                    weekdays = [d for d in weekdays if d in available_weekdays]

                if weekdays:
                    filtered = filtered[filtered["day_of_week_abbr"].isin(weekdays)]
                    # logger.info(
                    #     f"Filtered on weekdays: {weekdays} | Shape: {filtered.shape}"
                    # )

        # Clean up temporary columns
        original_cols = set(df.columns)
        current_cols = set(filtered.columns)
        cols_to_drop = (
            current_cols - original_cols - {"month", "day_of_week"}
        )  # Keep these if they existed
        if cols_to_drop:
            filtered = filtered.drop(columns=list(cols_to_drop))

        return filtered

    except Exception as e:
        logger.error(f"Time period filtering failed: {str(e)}")
        raise FilteringError(f"Time period filtering failed: {str(e)}")


def filter_by_date(df: pd.DataFrame, date: Optional[str]) -> pd.DataFrame:
    """Filter DataFrame by specific date

    Args:
        df: Input DataFrame
        date: Date string in YYYY-MM-DD format

    Returns:
        Filtered DataFrame

    Raises:
        FilteringError: If date format is invalid or column is missing
    """
    try:
        _validate_dataframe(df)

        if date is None:
            return df

        if not isinstance(date, str):
            raise FilteringError(f"Date must be string, got {type(date)}")

        try:
            datetime.strptime(date, "%Y-%m-%d")  # Validate format
        except ValueError:
            raise FilteringError(f"Invalid date format: {date}. Expected YYYY-MM-DD")

        if "date" not in df.columns:
            raise FilteringError("Column 'date' not found in DataFrame")

        if date not in df["date"].values:
            logger.warning(f"Date '{date}' not found in DataFrame")
            return df.iloc[0:0]  # Return empty DataFrame with same structure

        filtered = df[df["date"] == date].copy()
        # logger.info(f"Filtered on date: {date} | Shape: {filtered.shape}")
        return filtered

    except Exception as e:
        logger.error(f"Date filtering failed: {str(e)}")
        raise FilteringError(f"Date filtering failed: {str(e)}")


def filter_by_numeric_range(
    df: pd.DataFrame,
    column: Optional[str],
    min_val: Optional[Union[int, float]],
    max_val: Optional[Union[int, float]],
) -> pd.DataFrame:
    """Filter DataFrame by numeric range

    Args:
        df: Input DataFrame
        column: Column name to filter on
        min_val: Minimum value (inclusive)
        max_val: Maximum value (inclusive)

    Returns:
        Filtered DataFrame

    Raises:
        FilteringError: If column is missing or values are invalid
    """
    try:
        _validate_dataframe(df)

        if column is None:
            return df

        if column not in df.columns:
            raise FilteringError(f"Column '{column}' not found in DataFrame")

        filtered = df.copy()

        if min_val is not None:
            filtered = filtered[filtered[column] >= min_val]
            # logger.info(f"Applied {column} >= {min_val} | Shape: {filtered.shape}")

        if max_val is not None:
            filtered = filtered[filtered[column] <= max_val]
            # logger.info(f"Applied {column} <= {max_val} | Shape: {filtered.shape}")

        return filtered

    except Exception as e:
        logger.error(f"Numeric range filtering failed: {str(e)}")
        raise FilteringError(f"Numeric range filtering failed: {str(e)}")


def filter_by_location_type(
    df: pd.DataFrame, indoor: bool = True, outdoor: bool = True
) -> pd.DataFrame:
    """Filter activities by indoor/outdoor location type

    Args:
        df: Input DataFrame
        indoor: Include indoor activities
        outdoor: Include outdoor activities

    Returns:
        Filtered DataFrame

    Raises:
        FilteringError: If indoor column is missing or invalid
    """
    try:
        _validate_dataframe(df)

        if "indoor" not in df.columns:
            raise FilteringError("Column 'indoor' not found for location filtering")

        df = df.copy()
        df.loc[:, "indoor"] = pd.to_numeric(df["indoor"], errors="coerce")

        if indoor and outdoor:
            return df  # No filtering needed
        elif indoor:
            filtered = df[df["indoor"] == 1]
            # logger.info("Filtered for indoor activities only")
        elif outdoor:
            filtered = df[df["indoor"] == 0]
            # logger.info("Filtered for outdoor activities only")
        else:
            logger.warning("No location type selected - returning empty DataFrame")
            return df.iloc[0:0]

        logger.debug(f"Shape after location filtering: {filtered.shape}")
        return filtered

    except Exception as e:
        logger.error(f"Location type filtering failed: {str(e)}")
        raise FilteringError(f"Location type filtering failed: {str(e)}")


def filter_activities_data(
    df: pd.DataFrame,
    sport_type: Optional[str] = None,
    months: Optional[List[str]] = None,
    years: Optional[List[int]] = None,
    weekdays: Optional[List[str]] = None,
    date: Optional[str] = None,
    range_column: Optional[str] = None,
    range_min: Optional[Union[int, float]] = None,
    range_max: Optional[Union[int, float]] = None,
    indoor: bool = True,
    outdoor: bool = True,
) -> pd.DataFrame:
    """Comprehensive filtering of activities data

    Args:
        df: Input DataFrame containing activities data
        sport_type: Filter by specific sport type
        months: Filter by month abbreviations (e.g., ['Jan', 'Feb'])
        years: Filter by years (e.g., [2020, 2021])
        weekdays: Filter by weekday abbreviations (e.g., ['Mon', 'Tue'])
        date: Filter by specific date (YYYY-MM-DD)
        range_column: Column for numeric range filtering
        range_min: Minimum value for range filtering
        range_max: Maximum value for range filtering
        indoor: Include indoor activities
        outdoor: Include outdoor activities

    Returns:
        Filtered DataFrame

    Raises:
        FilteringError: If any filtering operation fails
    """

    try:
        _validate_dataframe(df)
        # logger.info(f"Initial DataFrame shape: {df.shape}")

        # Apply filters in optimal order (most restrictive first)
        if date:
            df = filter_by_date(df, date)
            if df.empty:
                return df

        if sport_type:
            df = filter_by_sport_type(df, sport_type)
            if df.empty:
                return df

        df = filter_by_time_period(df, years=years, months=months, weekdays=weekdays)
        if df.empty:
            return df

        if range_column:
            df = filter_by_numeric_range(df, range_column, range_min, range_max)
            if df.empty:
                return df

        df = filter_by_location_type(df, indoor, outdoor)

        # logger.info(f"Final DataFrame shape after filtering: {df.shape}\n"
        return df

    except Exception as e:
        logger.error(f"Activities data filtering failed: {str(e)}")
        raise FilteringError(f"Activities data filtering failed: {str(e)}")
