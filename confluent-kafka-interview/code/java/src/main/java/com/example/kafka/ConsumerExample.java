
package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Algorithm (Consumer):
 * 1) Join group and subscribe topic.
 * 2) Poll records; process safely.
 * 3) Commit offsets after processing (sync commit here).
 */
public class ConsumerExample {
    public static void main(String[] args) {
        final String bootstrap = System.getenv().getOrDefault("BOOTSTRAP", "localhost:9092");
        final String topic = System.getenv().getOrDefault("TOPIC", "orders");
        final String group = System.getenv().getOrDefault("GROUP", "java-consumers");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(r -> 
                    System.out.printf("Consumed %s[%d]@%d key=%s value=%s%n", r.topic(), r.partition(), r.offset(), r.key(), r.value())
                );
                consumer.commitSync();
            }
        }
    }
}
