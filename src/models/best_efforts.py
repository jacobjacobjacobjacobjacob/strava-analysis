# src/models/best_efforts.py
import pandas as pd
import json


class BestEfforts:
    def __init__(
        self,
        id: int,
        date: str,
        name: str,
        distance: int,
        time: int,
        pr_rank: int,
    ):
        self.id: int = id
        self.date: str = date
        self.name: str = name
        self.distance: int = distance
        self.time: int = time
        self.pr_rank: int = pr_rank

    def __repr__(self):
        return (
            f"BestEffort(id={self.id}, date='{self.date}', name='{self.name}', "
            f"distance={self.distance}, time={self.time}, pr_rank={self.pr_rank})"
        )

    @staticmethod
    def process_best_efforts(activity_id: int, best_efforts_list: list) -> pd.DataFrame:
        best_efforts_data = []  # List to hold the best efforts data

        best_efforts = json.dumps(best_efforts_list)

        best_efforts_data.append([activity_id, best_efforts])

        best_efforts = [
            {
                "id": effort["activity"]["id"],
                "date": effort["start_date_local"][:10],  # Extract YYYY-MM-DD
                "name": effort["name"],
                "distance": effort["distance"],
                "time": effort["moving_time"],
                "pr_rank": (
                    effort["pr_rank"] if effort["pr_rank"] is not None else 0
                ),  # Convert None to 0
            }
            for effort in best_efforts_list
        ]

        # Convert the list of dictionaries to a DataFrame
        best_efforts_df = pd.DataFrame(best_efforts)
  
        return best_efforts_df
