# src/models/gear.py
import pandas as pd



class Gear:
    def __init__(
        self,
        gear_id: str,
        name: str,
        distance: float,
        brand_name: str,
        model_name: str,
        retired: bool,
        weight: float = None,
    ):
        self.gear_id: str = gear_id
        self.name: str = name
        self.distance: float = distance
        self.brand_name: str = brand_name
        self.model_name: str = model_name
        self.retired: bool = retired
        self.weight: float = weight

    def __repr__(self):
        return (
            f"Gear({self.gear_id}, {self.name}, {self.brand_name}, {self.model_name})"
        )

    @staticmethod
    def process_gears(client, df: pd.DataFrame):
        """Fetch gear details"""
        gear_ids = df["gear_id"].dropna().unique().tolist()


        gear_data = []  # List to hold the gear details

        # Collect gear data
        for gear_id in gear_ids:
            gear_details = client.get_gear_details(gear_id)

            gear_data.append(
                [
                    gear_id,
                    gear_details["name"],
                    gear_details["distance"],
                    gear_details["brand_name"],
                    gear_details["model_name"],
                    gear_details["retired"],
                    gear_details.get("weight"),  
                ]
            )

        # Convert the list of lists to a DataFrame
        gear_df = pd.DataFrame(
            gear_data,
            columns=[
                "gear_id",
                "name",
                "distance",
                "brand_name",
                "model_name",
                "retired",
                "weight",
            ],
        )
        gear_df["retired"] = gear_df["retired"].astype(int)  # Convert boolean to integer
        return gear_df
