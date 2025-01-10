# src/models/weather.py
from datetime import datetime, timedelta
from loguru import logger
# from src.queries import check_weather_ids, get_weather_params_from_db



class Weather:
    def __init__(
        self,
        activity_id,
        date,
        lat,
        lon,
        time,
        temp,
        weather_type,
        precipitation,
        rain,
        wind,
        snow,
    ):
        self.activity_id = activity_id
        self.date = date
        self.time = time
        self.lat = lat
        self.lon = lon
        self.temp = temp
        self.weather_type = weather_type
        self.precipitation = precipitation
        self.rain = rain
        self.wind = wind
        self.snow = snow

    def __repr__(self):
        return (
            f"Weather(activity_id={self.activity_id}, date='{self.date}', temp={self.temp}, "
            f"weather_type='{self.weather_type}', precipitation={self.precipitation}, rain={self.rain}, "
            f"wind={self.wind}, snow={self.snow})"
        )
    
    def round_time_to_nearest_hour(self, time_str):
        # Convert the time string into a datetime object
        time_obj = datetime.strptime(time_str, "%H:%M")

        # Round to the nearest hour
        if time_obj.minute >= 30:
            # Add timedelta of one hour and set minutes to 00
            time_obj = time_obj + timedelta(hours=1)

        # Return the rounded time as a string with format HH:MM
        return time_obj.strftime("%H:00")
    
    def get_params(self, activity_id: int):
        # date, lat_lng = get_weather_params_from_db(activity_id)
        logger.info("Validate params!")
        return date, lat_lng

    @staticmethod
    def compare_ids():
        """Retrieves IDs from the database and checks if any is missing"""
        # missing_weather_data = check_weather_ids()

        if len(missing_weather_data) != 0:
            logger.warning(
                f"{len(missing_weather_data)} activities are missing weather data."
            )

        else:
            logger.success("Weather data is up to date. ")

    

        
        # for _, row in weather_df.iterrows():
        #     # Create a Weather instance from the row
        #     weather = cls(
        #         activity_id=row["id"],
        #         date=row["date"],
        #         temp=row["temp"],
        #         weather_type=row["weather_type"],
        #         precipitation=row["precipitation"],
        #         rain=row["rain"],
        #         wind=row["wind"],
        #         snow=row["snow"],
        #     )
