# src/models/split.py
class Split:
    def __init__(
        self,
        split_id,
        sport_type,
        splits_metric,
        laps,
        available_zones,
    ):
        self.split_id = split_id
        self.sport_type = sport_type
        self.splits_metric = splits_metric
        self.laps = laps
        self.available_zones = available_zones

    def __repr__(self):
        return (
            f"Split(split_id={self.split_id}, sport_type='{self.sport_type}', "
            f"splits_metric={self.splits_metric}, laps={self.laps}, "
            f"available_zones={self.available_zones})"
        )

    def save_to_db(self):
        """Method to save the split to the database."""
        from src.database.db import insert_split

        return insert_split(self.to_series())

    def to_series(self):
        """Convert the Split instance to a pandas Series for compatibility with insert_split."""
        import pandas as pd

        return pd.Series(
            {
                "id": self.split_id,
                "sport_type": self.sport_type,
                "splits_metric": self.splits_metric,
                "laps": self.laps,
                "available_zones": self.available_zones,
            }
        )

    @classmethod
    def process_splits(cls, splits_df):
        """Process and save splits from a DataFrame to the database."""
        new_split_ids = []

        for _, row in splits_df.iterrows():
            # Create a Split instance from the row
            split = cls(
                split_id=row["id"],  # Assuming each split has a unique ID
                sport_type=row["sport_type"],
                splits_metric=row["splits_metric"],
                laps=row["laps"],
                available_zones=row["available_zones"],
            )
            # Save the Split instance to the database
            if split.save_to_db():
                new_split_ids.append(split.split_id)

        return new_split_ids
