# models/gear.py


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
    
    
    def save_to_db(self):
        """Method to save gear to the database."""
        from database.db import insert_gear 
        insert_gear(self)

    @classmethod
    def process_gears(cls, client, gear_ids):
        """Fetch gear details and save to the database."""
        for gear_id in gear_ids:
            gear_details = client.get_gear_details(gear_id)  

            # Create Gear instance
            gear = cls(
                gear_id=gear_details['id'],
                name=gear_details['name'],
                distance=gear_details['distance'],
                brand_name=gear_details['brand_name'],
                model_name=gear_details['model_name'],
                retired=gear_details['retired'],
                weight=gear_details.get('weight')  # Use .get() to avoid KeyError
            )

            gear.save_to_db()
