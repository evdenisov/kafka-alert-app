package com.example;

import com.example.models.Product;
import com.example.models.Purchase;
import com.example.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;
import java.util.Random;

public class DataGenerator {
    
    private static final String[] PRODUCT_NAMES = {
        "Laptop", "Smartphone", "Tablet", "Headphones", "Monitor",
        "Keyboard", "Mouse", "Printer", "Camera", "Speaker"
    };
    
    private static final String[] PRODUCT_DESCRIPTIONS = {
        "High-quality product", "Premium edition", "Standard version",
        "Professional grade", "Consumer model"
    };

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        Producer<Long, Product> productProducer = new KafkaProducer<>(
            props, new LongSerializer(), new JsonSerializer<Product>()
        );
        
        Producer<Long, Purchase> purchaseProducer = new KafkaProducer<>(
            props, new LongSerializer(), new JsonSerializer<Purchase>()
        );

        Random random = new Random();

        // Generate products
        System.out.println("Generating products...");
        for (long i = 0; i <= 100; i++) {
            Product product = new Product(
                i,
                PRODUCT_NAMES[random.nextInt(PRODUCT_NAMES.length)] + " " + i,
                PRODUCT_DESCRIPTIONS[random.nextInt(PRODUCT_DESCRIPTIONS.length)],
                50 + random.nextDouble() * 20
            );
            
            productProducer.send(new ProducerRecord<>("products", i, product));
        }
        productProducer.flush();
        System.out.println("Products generated!");

        // Generate purchases
        System.out.println("Generating purchases...");
        long purchaseId = 0;
        while (true) {
            Purchase purchase = new Purchase(
                purchaseId++,
                1 + random.nextInt(11),
                random.nextInt(101)
            );
            
            purchaseProducer.send(new ProducerRecord<>("purchases", purchaseId, purchase));
            
            Thread.sleep(random.nextInt(2000));
            
            if (purchaseId % 100 == 0) {
                System.out.println("Generated " + purchaseId + " purchases");
            }
        }
    }
}