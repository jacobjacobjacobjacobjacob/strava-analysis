# src/models/weather.py
class Weather:
    def __init__(
        self,
        activity_id,
        date,
        temp,
        weather_type,
        precipitation,
        rain,
        wind,
        snow,
    ):
        self.activity_id = activity_id
        self.date = date
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

    def save_to_db(self):
        """Method to save weather data to the database."""
        from src.database.db import insert_weather

        insert_weather(self)

    @classmethod
    def process_weather_data(cls, weather_df):
        """Process and save weather data from a DataFrame to the database."""
        for _, row in weather_df.iterrows():
            # Create a Weather instance from the row
            weather = cls(
                activity_id=row["id"],
                date=row["date"],
                temp=row["temp"],
                weather_type=row["weather_type"],
                precipitation=row["precipitation"],
                rain=row["rain"],
                wind=row["wind"],
                snow=row["snow"],
            )
            # Save the Weather instance to the database
            weather.save_to_db()