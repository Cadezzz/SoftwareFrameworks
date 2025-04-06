using Confluent.Kafka;
using Newtonsoft.Json;

namespace WeatherBackend.Services;

public class KafkaProducerService : BackgroundService
{
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly IOpenWeatherClient _openWeatherClient;
    private IProducer<string, string> _producer;
    ProducerConfig _config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092"
    };
    string[] cities = ["Vienna", "Krems", "Linz", "Eisenstadt", "Graz", "Salzburg", "Klagenfurt", "Innsbruck", "Bregenz"];

    public KafkaProducerService(ILogger<KafkaProducerService> logger, IOpenWeatherClient openWeatherClient)
    {
        _logger = logger;
        _openWeatherClient = openWeatherClient;
        _producer = new ProducerBuilder<string, string>(_config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Producer Service is starting.");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                foreach (var city in cities)
                {
                    var weatherData = await _openWeatherClient.GetWeatherDataAsync(city);
                    if (weatherData != null)
                    {
                        await ProduceWeatherDataAsync(weatherData);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when the service is stopping
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while producing message.");
            }
        }
    }

    public async Task ProduceWeatherDataAsync(WeatherData weatherData)
    {
        await SendMessageAsync($"weather-{weatherData.CityName.ToLowerInvariant()}", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"), JsonConvert.SerializeObject(weatherData));
    }

    private async Task SendMessageAsync(string topic, string key, string value)
    {
        var message = new Message<string, string> { Key = key, Value = value };
        var result = await _producer.ProduceAsync(topic, message);
    }
}
