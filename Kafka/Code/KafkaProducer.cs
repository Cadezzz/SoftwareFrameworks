using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka // Update namespace based on your folder structure
{
    class KafkaProducer
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092" 
            };

            using var producer = new ProducerBuilder<string, string>(config).Build();

            Console.WriteLine("Producing messages...");
            
            for (int i = 0; i < 5; i++)
            {
                var message = new Message<string, string>
                {
                    Key = i.ToString(),
                    Value = "hello"
                };

                var result = await producer.ProduceAsync("test-topic", message);
                Console.WriteLine($"Produced message: Key = {message.Key}, Value = {message.Value}, Partition = {result.Partition}");
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine("Done producing messages.");
        }
    }
}