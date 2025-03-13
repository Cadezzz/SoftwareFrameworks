using System;
using System.Threading.Tasks;

namespace Kafka
{
    class KafkaApp
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Kafka Producer...");

           
            await KafkaProducer.RunProducer(args);

            Console.WriteLine("Producer finished. Now starting Kafka Consumer...");

            
            KafkaConsumer.RunConsumer(args);
        }
    }
}