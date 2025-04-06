namespace WeatherBackend.Services;

public interface IOpenWeatherClient
{
    Task<WeatherData?> GetWeatherDataAsync(string cityName);
}
