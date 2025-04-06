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
                services.AddHostedService<KafkaProducerService>();
                services.AddHostedService<KafkaConsumerService>();
                services.AddSingleton<ISQLRepository, SQLRepository>();
                services.AddSingleton<IOpenWeatherClient, OpenWeatherClient>();
            })
            .Build();
        await host.RunAsync();
    }
}
