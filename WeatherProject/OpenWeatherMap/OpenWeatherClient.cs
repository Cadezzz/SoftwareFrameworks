using System.Text.Json;

namespace WeatherProject.OpenWeatherMap;

public class OpenWeatherClient
{
    private static readonly HttpClient client = new HttpClient();
    private readonly string apiKey = "197688b2c19ec60e11bddf5448ebb1d0";

    public async Task<WeatherData?> GetWeatherDataAsync(string cityName)
    {
        var apiUrl = $"https://api.openweathermap.org/data/2.5/weather?q={cityName}&appid={apiKey}&units=metric";
        var data = string.Empty;
        try
        {
            HttpResponseMessage httpResponse = await client.GetAsync(apiUrl);
            if (!httpResponse.IsSuccessStatusCode)
            {
                Console.WriteLine($"Error fetching data: {httpResponse.StatusCode}");
                return null;
            }
            data = await httpResponse.Content.ReadAsStringAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Exception occurred: {ex.Message}");
        }

        using (JsonDocument document = JsonDocument.Parse(data))
        {
            TemperatureData temperatureData = MapTemperatureData(document);
            HumidityData humidityData = MapHumidityData(document);
            WindData windData = MapWindData(document);
            RainData rainData = MapRainData(document);
            return new WeatherData(cityName, DateTime.UtcNow, temperatureData.Temperature, temperatureData.TemperatureFeelsLike, humidityData.Humidity, windData.WindSpeed, windData.WindDegrees, rainData.RainFall);
        }
    }

    private TemperatureData MapTemperatureData(JsonDocument document)
    {
        JsonElement root = document.RootElement;
        double temp = root.GetProperty("main").GetProperty("temp").GetDouble();
        double feels_like = root.GetProperty("main").GetProperty("feels_like").GetDouble();
        return new TemperatureData(temp, feels_like);
    }

    private HumidityData MapHumidityData(JsonDocument document)
    {
        JsonElement root = document.RootElement;
        int humidity = root.GetProperty("main").GetProperty("humidity").GetInt32();
        return new HumidityData(humidity);
    }

    private WindData MapWindData(JsonDocument document)
    {
        JsonElement root = document.RootElement;
        double speed = root.GetProperty("wind").GetProperty("speed").GetDouble();
        int deg = root.GetProperty("wind").GetProperty("deg").GetInt32();
        return new WindData(speed, deg);
    }

    private RainData MapRainData(JsonDocument document)
    {
        JsonElement root = document.RootElement;
        // Rainfall data might not be present if there is no rain.
        double rainfall = 0;
        if (root.TryGetProperty("rain", out JsonElement rainElement))
        {
            // The API may return rainfall over the last 1 hour ("1h") or 3 hours ("3h")
            if (rainElement.TryGetProperty("1h", out JsonElement rain1h))
            {
                rainfall = rain1h.GetDouble();
            }
            else if (rainElement.TryGetProperty("3h", out JsonElement rain3h))
            {
                rainfall = rain3h.GetDouble();
            }
        }
        return new RainData(rainfall);
    }
}
