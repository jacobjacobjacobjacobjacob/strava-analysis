# src/clients/strava_client.py
import requests
from loguru import logger
from src.clients.streams import StreamClient
from src.utils import timer


class StravaClient:
    def __init__(
        self,
        client_id,
        client_secret,
        refresh_token,
        athlete_id,
        access_token=None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = access_token
        self.athlete_id = athlete_id
        self.stream_client = StreamClient(self)
        logger.info(f"Initializing StravaClient for athlete {athlete_id}")

        if self.access_token is None:
            self.refresh_access_token()

    def refresh_access_token(self):
        """Refresh the access token using the refresh token."""
        url = "https://www.strava.com/oauth/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        response = requests.post(url, params=params)
        if response.status_code == 200:
            data = response.json()
            self.access_token = data["access_token"]
            self.refresh_token = data["refresh_token"]
            self.expires_at = data["expires_at"]
            logger.debug("Access token refreshed successfully.")

        else:
            logger.error(f"Failed to refresh token: {e}")

    # def make_request(self, endpoint, method="GET", params=None):
    #     """Make a request to the Strava API and return the JSON response."""

    #     headers = {"Authorization": f"Bearer {self.access_token}"}

    #     url = f"https://www.strava.com/api/v3/{endpoint}"

    #     try:
    #         if method == "GET":
    #             response = requests.get(url, headers=headers, params=params)
    #         elif method == "POST":
    #             response = requests.post(url, headers=headers, json=params)
    #         else:
    #             raise ValueError(f"HTTP method {method} not supported.")

    #         response.raise_for_status()  # Raise an error for bad status codes
    #         return response.json()
    #     except requests.exceptions.RequestException as e:
    #         logger.error(f"Request to {endpoint} failed: {e}")
    #         raise
    def make_request(self, endpoint, method="GET", params=None):
        """Make a paginated request to the Strava API and return all results."""

        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"https://www.strava.com/api/v3/{endpoint}"
        all_data = []
        page = 1  
        params = params or {}  #

        try:
            while True:
                params.update({"page": page, "per_page": 200})
                if method == "GET":
                    response = requests.get(url, headers=headers, params=params)
                elif method == "POST":
                    response = requests.post(url, headers=headers, json=params)
                else:
                    raise ValueError(f"HTTP method {method} not supported.")

                response.raise_for_status()  # Raise an error for bad status codes
                data = response.json()

                if not data:  # Exit if no more data
                    break

                all_data.extend(data)  # Append current page data
                page += 1  # Move to the next page

            return all_data  # Return all paginated data

        except requests.exceptions.RequestException as e:
            logger.error(f"Request to {endpoint} failed: {e}")
            raise

    @timer
    def get_activities(self, per_page=200):
        """Fetch the athlete's activities."""
        params = {"per_page": per_page}
        return self.make_request("athlete/activities", params=params)

    def get_detailed_activity(self, activity_id):
        """Fetch details of a specific activity by ID."""
        return self.make_request(f"activities/{activity_id}")

    def get_activity_zones(self, activity_id):
        """Fetch heart rate and power zones for a specific activity."""
        return self.make_request(f"activities/{activity_id}/zones")

    def get_activity_laps(self, activity_id):
        """Fetch lap details of a specific activity by ID."""
        return self.make_request(f"activities/{activity_id}/laps")

    def get_athlete_stats(self):
        """Fetch activity stats for the athlete"""
        return self.make_request(f"athletes/{self.athlete_id}/stats")

    def get_gear_details(self, gear_id):
        """Fetch details of a specific gear item by ID."""
        return self.make_request(f"gear/{gear_id}")
