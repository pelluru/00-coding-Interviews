
package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * Algorithm (Producer):
 * 1) Configure idempotent producer with acks=all.
 * 2) Send keyed records (ordering per key).
 * 3) Use batching (linger.ms, batch.size) for throughput.
 * 4) Handle callbacks and close producer to flush.
 */
public class ProducerExample {
    public static void main(String[] args) {
        final String bootstrap = System.getenv().getOrDefault("BOOTSTRAP", "localhost:9092");
        final String topic = System.getenv().getOrDefault("TOPIC", "orders");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

        try (KafkaProducer<String,String> producer = new KafkaProducer<>(props)) {
            for (int i=0; i<10; i++) {
                String key = "user-" + (i % 3);
                String val = "{\"event\":\"order_created\",\"id\":" + i + "}";
                ProducerRecord<String,String> rec = new ProducerRecord<>(topic, key, val);
                producer.send(rec, (md, ex) -> {
                    if (ex != null) ex.printStackTrace();
                    else System.out.printf("Produced %s[%d]@%d key=%s%n", md.topic(), md.partition(), md.offset(), key);
                });
            }
            producer.flush();
        }
    }
}
