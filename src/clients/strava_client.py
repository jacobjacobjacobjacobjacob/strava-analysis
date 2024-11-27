# src/clients/strava_client.py
import requests
from loguru import logger
from clients.streams_client import StreamClient
from src.utils import timer, VALID_STREAM_TYPES


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
            logger.info("Access token refreshed successfully.")

        else:
            logger.error(f"Failed to refresh token: {e}")

    def make_request(self, endpoint, method="GET", params=None):
        """Make a request to the Strava API and return the JSON response."""

        headers = {"Authorization": f"Bearer {self.access_token}"}

        url = f"https://www.strava.com/api/v3/{endpoint}"

        try:
            if method == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method == "POST":
                response = requests.post(url, headers=headers, json=params)
            else:
                raise ValueError(f"HTTP method {method} not supported.")

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to {endpoint} failed: {e}")

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

    def get_activity_available_streams(self, activity_id, possible_streams=None):
        """Get the available stream types for a specific activity."""
        params = {"keys": ",".join(VALID_STREAM_TYPES), "key_by_type": True}
        endpoint = f"activities/{activity_id}/streams"

        response = self.make_request(endpoint, params=params)
        if response:
            available_streams = list(response.keys())
            logger.info(
                f"Available streams for activity {activity_id}: {available_streams}"
            )
            return available_streams
        else:
            logger.error(f"Failed to retrieve streams for activity {activity_id}")
            return []
