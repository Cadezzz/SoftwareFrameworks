public record WeatherData(TemperatureData TemperatureData, HumidityData HumidityData, WindData WindData, RainData RainData);

public record TemperatureData(DateTime UTCTimestamp, double Temperature, double TemperatureFeelsLike);

public record HumidityData(DateTime UTCTimestamp, int Humidity);

public record WindData(DateTime UTCTimestamp, double WindSpeed, int WindDegrees);

public record RainData(DateTime UTCTimestamp, double RainFall);
