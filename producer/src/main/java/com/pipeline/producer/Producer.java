package com.pipeline.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "sample-events");

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);

        System.out.println("Starting producer...");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Topic: " + topic);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int eventId = 0;
            while (true) {
                String key = "event-" + (eventId % 3); // distribute across 3 partitions
                String value = String.format(
                        "{\"eventId\":%d,\"type\":\"sample\",\"source\":\"producer-v1\",\"timestamp\":%d}",
                        eventId, System.currentTimeMillis());

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                Future<RecordMetadata> future = producer.send(record);

                RecordMetadata metadata = future.get();
                System.out.printf("Sent: partition=%d offset=%d | %s%n",
                        metadata.partition(), metadata.offset(), value);

                eventId++;
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
