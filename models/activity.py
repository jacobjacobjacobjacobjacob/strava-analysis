# models/activity.py
class Activity:
    def __init__(
        self,
        activity_id,
        date,
        month,
        day_of_week,
        start_time,
        end_time,
        sport_type,
        indoor,
        distance,
        duration,
        elevation_gain,
        gear_id,
        average_heartrate,
        average_speed,
        average_cadence,
        average_temp,
        average_watts,
        intensity,
    ):
        self.activity_id = activity_id
        self.date = date
        self.month = month
        self.day_of_week = day_of_week
        self.start_time = start_time
        self.end_time = end_time
        self.sport_type = sport_type
        self.indoor = indoor
        self.distance = distance
        self.duration = duration
        self.elevation_gain = elevation_gain
        self.gear_id = gear_id
        self.average_heartrate = average_heartrate
        self.average_speed = average_speed
        self.average_cadence = average_cadence
        self.average_temp = average_temp
        self.average_watts = average_watts
        self.intensity = intensity

    def __repr__(self):
        return (
            f"Activity(activity_id={self.activity_id}, date='{self.date}', month='{self.month}', "
            f"day_of_week='{self.day_of_week}', start_time='{self.start_time}', end_time='{self.end_time}', "
            f"sport_type='{self.sport_type}', indoor={self.indoor}, distance={self.distance}, "
            f"duration={self.duration}, elevation_gain={self.elevation_gain}, gear_id='{self.gear_id}', "
            f"average_heartrate={self.average_heartrate}, average_speed={self.average_speed}, "
            f"average_cadence={self.average_cadence}, average_temp={self.average_temp}, "
            f"average_watts={self.average_watts}, intensity={self.intensity})"
        )

    def save_to_db(self):
        """Method to save activities to the database."""
        from database.db import insert_activity

        insert_activity(self)

    @classmethod
    def process_activities(cls, activities_df):
        """Process and save activities from a DataFrame to the database."""
        for index, row in activities_df.iterrows():
            # Create an Activity instance from the row
            activity = cls(
                activity_id=row["id"],
                date=row["date"],
                month=row["month"],
                day_of_week=row["day_of_week"],
                start_time=row["start_time"],
                end_time=row["end_time"],
                sport_type=row["sport_type"],
                indoor=row["indoor"],
                distance=row["distance"],
                duration=row["duration"],
                elevation_gain=row["elevation_gain"],
                gear_id=row["gear_id"],
                average_heartrate=row["average_heartrate"],
                average_speed=row["average_speed"],
                average_cadence=row["average_cadence"],
                average_temp=row["average_temp"],
                average_watts=row["average_watts"],
                intensity=row["intensity"],
            )
            # Save the Activity instance to the database
            activity.save_to_db()
