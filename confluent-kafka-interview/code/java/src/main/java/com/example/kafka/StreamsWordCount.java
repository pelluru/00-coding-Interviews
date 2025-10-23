
package com.example.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Streams WordCount
 * Algorithm:
 * 1) Read text lines.
 * 2) FlatMap to words.
 * 3) GroupByKey and count with tumbling window.
 * 4) Materialize to a state store; results emit to output topic.
 */
public class StreamsWordCount {
    public static void main(String[] args) {
        final String bootstrap = System.getenv().getOrDefault("BOOTSTRAP", "localhost:9092");
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> lines = b.stream("text-input");

        KTable<Windowed<String>, Long> counts = lines
            .flatMapValues(v -> java.util.Arrays.asList(v.toLowerCase().split("\\W+")))
            .groupBy((k, word) -> word)
            .windowedBy(TimeWindows.ofSizeWithNoGrace(java.time.Duration.ofSeconds(30)))
            .count(Materialized.as("counts"));

        counts.toStream().map((w, c) -> KeyValue.pair(w.key(), c.toString())).to("wordcount-output");

        KafkaStreams app = new KafkaStreams(b.build(), props);
        app.start();
        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
    }
}
