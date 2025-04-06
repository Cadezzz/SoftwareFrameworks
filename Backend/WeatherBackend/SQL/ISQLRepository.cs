namespace WeatherBackend.SQL;

public interface ISQLRepository
{
    Task EnsureDatabaseExistsAsync();
    Task InsertWeatherDataAsync(WeatherData weatherData);
}