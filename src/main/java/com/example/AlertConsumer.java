package com.example;

import com.example.models.Alert;
import com.example.serde.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AlertConsumer {
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.CONFIG_VALUE_CLASS, Alert.class.getName());

        KafkaConsumer<String, Alert> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("revenue-alerts"));

        System.out.println("Listening for revenue alerts...");
        
        while (true) {
            ConsumerRecords<String, Alert> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Alert> record : records) {
                Alert alert = record.value();
                System.out.println("🚨 ALERT RECEIVED: " + alert);
            }
        }
    }
}