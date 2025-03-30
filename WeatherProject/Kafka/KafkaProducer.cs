using Confluent.Kafka;
using System.Text.Json;

namespace WeatherProject.Kafka;

class KafkaProducer
{
    ProducerConfig config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092"
    };
    IProducer<string, string> producer;

    public async Task ProduceWeatherDataAsync(WeatherData weatherData)
    {
        using (producer = new ProducerBuilder<string, string>(config).Build())
        {
            await SendMessageAsync($"weather-{weatherData.CityName.ToLowerInvariant()}", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"), JsonSerializer.Serialize(weatherData));
        }
    }

    private async Task SendMessageAsync(string topic, string key, string value)
    {
        var message = new Message<string, string> { Key = key, Value = value };
        var result = await producer.ProduceAsync(topic, message);
    }
}
