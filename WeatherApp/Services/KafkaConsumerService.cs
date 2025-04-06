using Confluent.Kafka;
using Newtonsoft.Json;
using WeatherBackend.SQL;

namespace WeatherBackend.Services;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly ISQLRepository _databaseRepository;
    private IConsumer<Ignore, string> _consumer;
    private IProducer<string, string> _producer;
    private Dictionary<string, List<double>> temperatureAggregate = new Dictionary<string, List<double>>();

    public KafkaConsumerService(ILogger<KafkaConsumerService> logger, ISQLRepository databaseRepository)
    {
        _logger = logger;
        _databaseRepository = databaseRepository;

        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "weather-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        _consumer.Subscribe(new[] { "weather-vienna", "weather-krems", "weather-linz", "weather-eisenstadt", "weather-graz", "weather-klagenfurt", "weather-salzburg", "weather-innsbruck", "weather-bregenz" });
        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Consumer Service is starting.");
        await _databaseRepository.EnsureDatabaseExistsAsync();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(stoppingToken);
                if (consumeResult != null)
                {
                    _logger.LogInformation($"Message received: {consumeResult.Message.Value}");
                    var wd = JsonConvert.DeserializeObject<WeatherData>(consumeResult.Message.Value);
                    if (wd != null)
                    {
                        await _databaseRepository.InsertWeatherDataAsync(wd);
                        await AddTemperatureAsync(wd.CityName, wd.Temperature);
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
                _logger.LogError(ex, "Error occurred while consuming message.");
            }
        }

        _consumer.Close();
        _logger.LogInformation("Kafka Consumer Service is stopping.");
    }

    private async Task AddTemperatureAsync(string city, double temperature)
    {
        if (!temperatureAggregate.ContainsKey(city))
        {
            temperatureAggregate[city] = new List<double>();
        }

        temperatureAggregate[city].Add(temperature);

        if (temperatureAggregate[city].Count == 10)
        {
            double averageTemperature = temperatureAggregate[city].Average();
            averageTemperature = Math.Round(averageTemperature, 2);
            await SendMessageAsync($"avg-temp-{city.ToLowerInvariant()}", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"), averageTemperature.ToString());
            temperatureAggregate[city].Clear();
        }
    }

    private async Task SendMessageAsync(string topic, string key, string value)
    {
        var message = new Message<string, string> { Key = key, Value = value };
        var result = await _producer.ProduceAsync(topic, message);
    }
}
