public record WeatherData(string CityName, DateTime TimestampUTC, double Temperature, double TemperatureFeelsLike, int Humidity, double WindSpeed, int WindDegrees, double RainFall);

public record TemperatureData(double Temperature, double TemperatureFeelsLike);

public record HumidityData(int Humidity);

public record WindData(double WindSpeed, int WindDegrees);

public record RainData(double RainFall);
