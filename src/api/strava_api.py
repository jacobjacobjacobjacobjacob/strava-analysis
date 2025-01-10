# src/clients/strava_client.py
import requests
import sys
import time
from loguru import logger
# from src.utils import check_rate_limit


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
        logger.success(f"Initializing StravaClient for athlete {athlete_id}")

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
        try:
            response = requests.post(url, params=params)
            response.raise_for_status()  # Ensure the request succeeded

            data = response.json()
            self.access_token = data["access_token"]
            self.refresh_token = data["refresh_token"]
            self.expires_at = data["expires_at"]
            logger.info("Access token refreshed successfully.")

        except requests.exceptions.RequestException as e:
            logger.critical(f"Failed to refresh token: {e}")

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

            self.check_rate_limit(response)
            response.raise_for_status()

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to {endpoint} failed: {e}")

    def get_activities(self, per_page=200, max_activities=None):
        """
        Fetch the athlete's activities, supporting pagination.

        :param per_page: Number of activities to fetch per page (default is 200).
        :param max_activities: Maximum number of activities to fetch (default is None for all available).
        :return: A list of activities.
        """
        activities = []
        page = 1

        while True:
            params = {"per_page": per_page, "page": page}

            data = self.make_request("athlete/activities", params=params)

            if not data:
                logger.warning("No activity data recieved.")
                break

            activities.extend(data)

            # Stop if we've fetched enough activities
            if max_activities and len(activities) >= max_activities:
                activities = activities[:max_activities]
                break

            # Stop if fewer activities are returned than requested (end of data)
            if len(data) < per_page:
                break

            page += 1

        return activities

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
    
    def check_rate_limit(self, response) -> None:
        """Check and handle Strava API rate limits."""
        if response is None:
            logger.error("No response received, cannot check rate limit.")
            return

        if response.status_code == 429:
            logger.critical("Rate limit exceeded. Waiting for 5 minutes...")
            time.sleep(5 * 60)

        usage = response.headers.get("X-RateLimit-Usage", "0,0").split(",")

        short_limit, daily_limit = 100, 1000
        short_usage, daily_usage = map(int, usage)

        logger.trace(
            f"Rate limit: {short_usage}/{short_limit} (15-min), {daily_usage}/{daily_limit} (daily)"
        )

        if daily_usage >= daily_limit:
            logger.critical("Daily limit reached. Exiting.")
            sys.exit()

        if short_usage >= short_limit - 1:  # If we're near the short-term limit
            logger.warning("Approaching 15-minute rate limit. Pausing for 15 minutes.")
            time.sleep(15 * 60)  # Sleep for 15 minutes

