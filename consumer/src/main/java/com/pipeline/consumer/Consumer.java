package com.pipeline.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "sample-events");
        String groupId = System.getenv().getOrDefault("KAFKA_GROUP_ID", "sample-consumer-group");

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");

        System.out.println("Starting consumer...");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Topic: " + topic);
        System.out.println("Group ID: " + groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            long totalMessages = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    totalMessages++;
                    System.out.printf("Received: partition=%d offset=%d key=%s | %s [total=%d]%n",
                            record.partition(), record.offset(), record.key(),
                            record.value(), totalMessages);
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
