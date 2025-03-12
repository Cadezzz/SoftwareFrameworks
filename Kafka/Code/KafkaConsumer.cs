using Confluent.Kafka;
namespace Kafka;

public class KafkaConsumer
{
    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("test-topic");

            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Received: {consumeResult.Message.Value}");
            }
        }
    }
}