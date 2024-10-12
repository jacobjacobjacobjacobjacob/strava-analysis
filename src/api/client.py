# src/api/client.py
import os
import sys
import requests
from dotenv import load_dotenv
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from assets.constants import VALID_STREAM_TYPES

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

            response.raise_for_status()  # Raise an error for bad status codes

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to {endpoint} failed: {e}")
            raise


    def get_activities(self, per_page=200):
        """Fetch the athlete's activities."""
        params = {"per_page": per_page}
        return self.make_request("athlete/activities", params=params)

    def get_detailed_activity(self, activity_id):
        """Fetch details of a specific activity by ID."""
        return self.make_request(f"activities/{activity_id}")

    def get_activity_laps(self, activity_id):
        """Fetch lap details of a specific activity by ID."""
        return self.make_request(f"activities/{activity_id}/laps")

    def get_athlete_stats(self):
        """Fetch activity stats for the athlete"""
        return self.make_request(f"athletes/{self.athlete_id}/stats")
    
    def get_gear_details(self, gear_id):
        return self.make_request(f"gear/{gear_id}")


    # Streams
    def get_streams(self, activity_id, stream_types):
        """Private method to fetch multiple streams for a given activity."""
        # Validate stream types
        for stream_type in stream_types:
            if stream_type not in VALID_STREAM_TYPES:
                raise ValueError(f"Invalid stream type requested: {stream_type}")

        # Make API request using the validated stream types (comma-separated)
        stream_types_str = ",".join(stream_types)
        return self.make_request(f"activities/{activity_id}/streams?keys={stream_types_str}&key_by_type=true")
    
    def _get_streams(self, activity_id, stream_types):
        """Private method to fetch multiple streams for a given activity."""
        # Validate stream types
        for stream_type in stream_types:
            if stream_type not in VALID_STREAM_TYPES:
                raise ValueError(f"Invalid stream type requested: {stream_type}")

        # Make API request using the validated stream types (comma-separated)
        stream_types_str = ",".join(stream_types)
        return self.make_request(f"activities/{activity_id}/streams?keys={stream_types_str}&key_by_type=true")

  

    def get_time_stream(self, activity_id):
        # Fetch only the time stream
        return self._get_streams(activity_id, ["time"])

    def get_distance_stream(self, activity_id):
        # Fetch only the distance stream
        return self._get_streams(activity_id, ["distance"])

    def get_latlng_stream(self, activity_id):
        # Fetch only the latlng stream
        return self._get_streams(activity_id, ["latlng"])

    def get_altitude_stream(self, activity_id):
        # Fetch only the altitude stream
        return self._get_streams(activity_id, ["altitude"])

    def get_cadence_stream(self, activity_id):
        # Fetch only the cadence stream
        return self._get_streams(activity_id, ["cadence"])

    def get_speed_stream(self, activity_id):
        return self._get_stream(activity_id, "velocity_smooth")

    def get_heartrate_stream(self, activity_id):
        return self._get_stream(activity_id, "heartrate")


if __name__ == "__main__":
    load_dotenv()

    # Initialize Client
    client = StravaClient(
        client_id=os.getenv("STRAVA_CLIENT_ID"),
        client_secret=os.getenv("STRAVA_CLIENT_SECRET"),
        refresh_token=os.getenv("STRAVA_REFRESH_TOKEN"),
        athlete_id=os.getenv("STRAVA_ATHLETE_ID"),
    )

    # Fetch latest activities
    # activities = client.get_activities(per_page=5)
    # print(activities)

    # Get detailed activity
    #activity = client.get_detailed_activity(activity_id="12455871622")
    #print(activity)

    # Get athlete stats
    #stats = client.get_athlete_stats()
    #print(json.dumps(stats, indent=4))

    # laps = client.get_activity_laps(activity_id="12455871622")
    # print(json.dumps(laps, indent=4))

    # speed_stream = client.get_cadence_stream(activity_id="12433392206")
    # print(json.dumps(speed_stream, indent=4))
    
    def extract_stream_data(streams, stream_type):
        if not isinstance(streams, dict):
            raise ValueError("Expected streams to be a dictionary.")

        stream = streams.get(stream_type)
        if stream and isinstance(stream, dict):
            return stream.get('data', [])
        return []  # Return empty list if stream_type not found
    activity_id = "12422361892"
    stream_types = ["time", "distance"]

    streams_data = client.get_streams(activity_id, stream_types)

    # Print the whole streams_data to check its content
    #print(json.dumps(streams_data, indent=2))

    # Now try to extract time and distance streams
    time_stream = extract_stream_data(streams_data, "time")
    distance_stream = extract_stream_data(streams_data, "distance")

    #print("Time Stream:", time_stream)
    print("Distance Stream:", distance_stream)




    #print(time_stream)