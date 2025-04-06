using Microsoft.Data.SqlClient;
using System.Data;

namespace WeatherBackend.SQL;

public class SQLRepository : ISQLRepository
{
    private readonly ILogger<SQLRepository> _logger;
    private readonly string _connectionString = "Data Source=localhost,1433;Initial Catalog=Weather;User ID=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=True";

    public SQLRepository(ILogger<SQLRepository> logger)
    {
        _logger = logger;
        EnsureTableExists();
    }

    public Task SaveMessageAsync(string message)
    {

        _logger.LogInformation($"Saving message to the database: {message}");
        return Task.CompletedTask;
    }

    public async Task EnsureDatabaseExistsAsync()
    {
        _logger.LogInformation("Ensuring weather database exists.");
        try
        {
            using (SqlConnection connection = new SqlConnection("Data Source=localhost,1433;Initial Catalog=master;User ID=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=True"))
            {
                await connection.OpenAsync();

                string createWeatherDataTable = @"
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'Weather')
BEGIN
    CREATE DATABASE Weather;
END
";
                using (SqlCommand command = new SqlCommand(createWeatherDataTable, connection))
                {
                    await command.ExecuteNonQueryAsync();
                    Thread.Sleep(10000);
                    _logger.LogInformation("Database 'Weather' was created.");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred while creating weather table.");
        }
    }

    private void EnsureTableExists()
    {
        _logger.LogInformation("Ensuring weather table exists.");
        try
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                string createWeatherDataTable = @"
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'WeatherData')
BEGIN
    CREATE TABLE [dbo].[WeatherData](
	    [ID] [int] IDENTITY(1,1) NOT NULL,
	    [TimestampUTC] [datetime] NOT NULL,
	    [City] [nvarchar](50) NOT NULL,
	    [Temperature] [float] NULL,
	    [TemperatureFeelsLike] [float] NULL,
	    [Humidity] [int] NULL,
	    [WindSpeed] [float] NULL,
	    [WindDegrees] [int] NULL,
	    [RainFall] [float] NULL,
     CONSTRAINT [PK_WeatherData] PRIMARY KEY CLUSTERED 
    (
	    [ID] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
    ) ON [PRIMARY]
    ALTER TABLE [dbo].[WeatherData] ADD  CONSTRAINT [DF_WeatherData_TimestampUTC]  DEFAULT (getutcdate()) FOR [TimestampUTC]
END
";
                using (SqlCommand command = new SqlCommand(createWeatherDataTable, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred while creating weather table.");
        }
    }

    public async Task InsertWeatherDataAsync(WeatherData weatherData)
    {
        var recentWeatherData = await GetRecentWeatherDataAsync(weatherData.CityName);
        if (recentWeatherData != null && WeatherDataAlreadyExists(recentWeatherData, weatherData))
        {
            return;
        }

        var sqlQuery = @"
INSERT [dbo].[WeatherData]
    ([TimestampUTC]
    ,[City]
    ,[Temperature]
    ,[TemperatureFeelsLike]
    ,[Humidity]
    ,[WindSpeed]
    ,[WindDegrees]
    ,[RainFall])
VALUES
    (@TimestampUTC
    ,@City
    ,@Temperature
    ,@TemperatureFeelsLike
    ,@Humidity
    ,@WindSpeed
    ,@WindDegrees
    ,@RainFall)
";
        using (SqlConnection connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            using (SqlCommand command = new SqlCommand(sqlQuery, connection))
            {
                command.Parameters.Add("@TimestampUTC", SqlDbType.DateTime).Value = weatherData.TimestampUTC;
                command.Parameters.Add("@City", SqlDbType.NVarChar).Value = weatherData.CityName;
                command.Parameters.Add("@Temperature", SqlDbType.Float).Value = weatherData.Temperature;
                command.Parameters.Add("@TemperatureFeelsLike", SqlDbType.Float).Value = weatherData.TemperatureFeelsLike;
                command.Parameters.Add("@Humidity", SqlDbType.Int).Value = weatherData.Humidity;
                command.Parameters.Add("@WindSpeed", SqlDbType.Float).Value = weatherData.WindSpeed;
                command.Parameters.Add("@WindDegrees", SqlDbType.Int).Value = weatherData.WindDegrees;
                command.Parameters.Add("@RainFall", SqlDbType.Float).Value = weatherData.RainFall;
                await command.ExecuteNonQueryAsync();
            }
        }
    }

    private async Task<WeatherData?> GetRecentWeatherDataAsync(string city)
    {
        using (SqlConnection connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();

            string sqlQuery = @"
SELECT TOP 1
     [TimestampUTC]
    ,[Temperature]
    ,[TemperatureFeelsLike]
    ,[Humidity]
    ,[WindSpeed]
    ,[WindDegrees]
    ,[RainFall]
FROM
    [dbo].[WeatherData]
WHERE
    [City] = @city
ORDER BY
    [TimestampUTC] DESC
";

            using (SqlCommand command = new SqlCommand(sqlQuery, connection))
            {
                command.Parameters.Add("@city", SqlDbType.NVarChar).Value = city;
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var weatherData = new WeatherData(
                            city,
                            reader.GetDateTime("TimestampUTC"),
                            reader.GetDouble("Temperature"),
                            reader.GetDouble("TemperatureFeelsLike"),
                            reader.GetInt32("Humidity"),
                            reader.GetDouble("WindSpeed"),
                            reader.GetInt32("WindDegrees"),
                            reader.GetDouble("RainFall")
                        );
                        return weatherData;
                    }
                }
            }
        }
        return null;
    }

    private bool WeatherDataAlreadyExists(WeatherData? weatherDataDB, WeatherData weatherDataStream)
    {
        if (weatherDataDB.Temperature == weatherDataStream.Temperature
            && weatherDataDB.TemperatureFeelsLike == weatherDataStream.TemperatureFeelsLike
            && weatherDataDB.Humidity == weatherDataStream.Humidity
            && weatherDataDB.WindSpeed == weatherDataStream.WindSpeed
            && weatherDataDB.WindDegrees == weatherDataStream.WindDegrees
            && weatherDataDB.RainFall == weatherDataStream.RainFall)
        {
            return true;
        }
        return false;
    }
}
