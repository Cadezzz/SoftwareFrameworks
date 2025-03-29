import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.common.utils.Bytes;

import java.time.Duration;
import java.util.Properties;

public class KafkaStreamAggregator {

    public static void main(String[] args) {

        // Set Kafka Streams application properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:29092");
        properties.put("application.id", "temperature-aggregation-app");
        properties.put("default.key.serde", Serdes.String().getClass().getName());
        properties.put("default.value.serde", Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Define the input stream (consuming from "temperature-data" topic)
        KStream<String, String> stream = builder.stream("temperature-data");

        // Convert stream values to Integer (temperature values)
        KStream<String, Integer> parsedStream = stream.mapValues(value -> Integer.parseInt(value.split(":")[1]));

        // Aggregate the stream over a 5-minute window
        KTable<Windowed<String>, Double> aggregatedStream = parsedStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))  // 5-minute window
                .aggregate(
                        () -> 0.0,  // Initial aggregator state (sum of temperatures)
                        (key, value, aggregate) -> aggregate + value,  // Aggregate function
                        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("temperature-aggregation-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double()) // Ensure serialization of values
                )
                .mapValues(aggregate -> aggregate / 5.0);  // Compute average temperature

        // Write the aggregated stream to a new Kafka topic
        aggregatedStream.toStream().to("aggregated-temperature-data");

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        // Gracefully shutdown on termination
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
