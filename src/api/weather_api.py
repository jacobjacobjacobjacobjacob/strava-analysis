# src/clients/weather_client.py
import requests
from loguru import logger
# from src.queries import get_weather_params_from_db


class WeatherClient:

    def __init__(self):
        logger.success("Initializing WeatherClient")

    def make_request(self, params: dict):
        """Make a request to the OpenMeteo API and return the JSON response."""
        url = "https://api.open-meteo.com/v1/forecast?"

        try:
            response = requests.get(url, params=params)

            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")

    def get_weather_data(self, activity_id: int):
        print("PARAMS:")

        # date, lat_lng = get_weather_params_from_db(activity_id)
        print(date)
        print(lat_lng)
        lat = 0
        lng = 0
        params = {
            "latitude": lat,
            "longitude": lng,
            "hourly": ",".join(
                [
                    "temperature_2m",
                    "precipitation",
                    "weather_code",
                    "wind_speed_10m",
                    "rain",
                    "snowfall",
                ]
            ),
            "wind_speed_unit": "ms",
            "timezone": "Europe/Berlin",
            "start_date": date,
            "end_date": date,
        }
        return self.make_request(params)
