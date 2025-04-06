using Confluent.Kafka;
using Newtonsoft.Json;
using WeatherBackend.SQL;

namespace WeatherBackend.Services;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly ISQLRepository _databaseRepository;
    private IConsumer<Ignore, string> _consumer;

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

        _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        _consumer.Subscribe(new[] { "weather-vienna", "weather-krems", "weather-linz", "weather-eisenstadt", "weather-graz", "weather-klagenfurt", "weather-salzburg", "weather-innsbruck", "weather-bregenz" });
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
                        await _databaseRepository.InsertWeatherDataAsync(wd);
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
}
