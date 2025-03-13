using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka
{
    class KafkaProducer
    {
        public static async Task RunProducer(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092" // Ensure Kafka is running
            };

            using var producer = new ProducerBuilder<string, string>(config).Build();

            Console.WriteLine("Producing weather data messages...");

            // Weather data simulation
            Random random = new Random();
            for (int i = 0; i < 5; i++)
            {
                // Create sample weather readings
                var temperature = $"{random.Next(-10, 40)}°C";
                var humidity = $"{random.Next(20, 100)}%";
                var windSpeed = $"{random.Next(0, 30)} km/h";
                var rainfall = $"{random.Next(0, 100)} mm";

                // Send messages to different topics
                await SendMessage(producer, "temperature-data", i.ToString(), temperature);
                await SendMessage(producer, "humidity-data", i.ToString(), humidity);
                await SendMessage(producer, "wind-speed-data", i.ToString(), windSpeed);
                await SendMessage(producer, "rainfall-data", i.ToString(), rainfall);
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine("Done producing weather messages.");
        }

        private static async Task SendMessage(IProducer<string, string> producer, string topic, string key, string value)
        {
            var message = new Message<string, string> { Key = key, Value = value };
            var result = await producer.ProduceAsync(topic, message);
            Console.WriteLine($"[Sent] Topic: {topic} | Key: {message.Key} | Value: {message.Value} | Partition: {result.Partition}");
        }
    }
}