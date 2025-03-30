using WeatherBackend.Services;
using WeatherBackend.SQL;

namespace WeatherBackend;

public class Program
{
    public static async Task Main(string[] args)
    {
        IHost host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((hostContext, services) =>
            {
                services.AddHostedService<KafkaConsumerService>();
                services.AddSingleton<ISQLRepository, SQLRepository>();
            })
            .Build();
        await host.RunAsync();
    }
}