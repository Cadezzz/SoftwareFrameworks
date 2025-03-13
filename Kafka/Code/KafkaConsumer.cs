using System;
using Confluent.Kafka;

namespace Kafka
{
    public class KafkaConsumer
    {
        public static void RunConsumer(string[] args) // Renamed from Main
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "weather-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(new[] { "temperature-data", "humidity-data", "wind-speed-data", "rainfall-data" });

            Console.WriteLine("Listening for weather data...");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume();
                    Console.WriteLine($"[Received] Topic: {consumeResult.Topic} | Value: {consumeResult.Message.Value}");
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumer shutting down...");
                consumer.Close();
            }
        }
    }
}