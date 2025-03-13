package org.thingsboard.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class MLConsumer {
    private static final String IN_TOPIC = "telemetry-raw";
    private static final String OUT_TOPIC = "telemetry-predictions";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ml-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, TelemetryData> source = builder.stream(IN_TOPIC);

        KStream<String, TelemetryData> predictions = source.mapValues(value -> {
            // Simple linear regression model for prediction
            double predictedValue = value.getValue() * 1.1; // Example prediction logic
            TelemetryData predictedData = new TelemetryData();
            predictedData.setDeviceId(value.getDeviceId());
            predictedData.setTimestamp(value.getTimestamp() + 60000); // Predicting for the next minute
            predictedData.setValue(predictedValue);
            return predictedData;
        });

        predictions.to(OUT_TOPIC, Produced.with(Serdes.String(), new JsonPojoSerializer<>()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}