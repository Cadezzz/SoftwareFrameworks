namespace WeatherBackend.SQL;

public interface ISQLRepository
{
    Task InsertWeatherDataAsync(WeatherData weatherData);
}