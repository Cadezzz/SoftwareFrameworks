using WeatherProject.Kafka;
using WeatherProject.OpenWeatherMap;

namespace WeatherProject;

public class Program
{
    public static async Task Main()
    {
        var openWeatherClient = new OpenWeatherClient();
        var producer = new KafkaProducer();

        string[] cities = ["Vienna", "Graz", "Salzburg"];
        foreach (var city in cities)
        {
            var timer = new System.Timers.Timer(60000);
            timer.Elapsed += async (sender, e) =>
            {
                var weatherData = await openWeatherClient.GetWeatherDataAsync(city);
                if (weatherData != null)
                {
                    await producer.ProduceWeatherDataAsync(weatherData);
                }
            };
            timer.AutoReset = true;
            timer.Enabled = true;
        }
        Console.ReadLine();
    }
}
