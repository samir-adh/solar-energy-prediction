class WeatherStation:
    def __init__(self, name, latitude, longitude):
        self.name = name
        self.latitude = latitude
        self.longitude = longitude

    def __repr__(self):
        return f"WeatherStation(name={self.name}, latitude={self.latitude}, longitude={self.longitude})"