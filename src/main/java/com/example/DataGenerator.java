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
        "Laptop", "Smartphone", "Tablet", "Headphones", "Monitor"
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
        
        final int TOTAL_PRODUCTS = 5; // Ограничиваем до 5 продуктов

        // Generate products - ОГРАНИЧЕНО ДО 5
        System.out.println("Generating " + TOTAL_PRODUCTS + " products...");
        for (long i = 1; i <= TOTAL_PRODUCTS; i++) {
            // Первые 2 продукта с высокой ценой для тестирования алертов
            double price;
            if (i <= 2) {
                price = 1000.0 + random.nextDouble() * 500; // Высокая цена 1000-1500
            } else {
                price = 50 + random.nextDouble() * 50; // Обычная цена 50-100
            }
            
            Product product = new Product(
                i,
                PRODUCT_NAMES[(int)(i - 1)] + " Pro " + i, // Используем все 5 имен по порядку
                PRODUCT_DESCRIPTIONS[random.nextInt(PRODUCT_DESCRIPTIONS.length)],
                price
            );
            
            productProducer.send(new ProducerRecord<>("products", i, product));
            System.out.println("Generated product: " + product.getName() + 
                             " (ID: " + i + ") with price: " + String.format("%.2f", product.getPrice()));
        }
        productProducer.flush();
        System.out.println(TOTAL_PRODUCTS + " products generated!");

        // Generate purchases
        System.out.println("Generating purchases for " + TOTAL_PRODUCTS + " products...");
        long purchaseId = 0;
        while (true) {
            // ProductId от 1 до 5 (включительно)
            long productId = 1 + random.nextInt(TOTAL_PRODUCTS);
            
            // Для продуктов с высокой ценой генерируем большее количество
            int quantity;
            if (productId <= 2) {
                // Для дорогих продуктов (ID 1-2) - больше шансов на алерт
                quantity = 2 + random.nextInt(8); // Количество от 2 до 9
            } else {
                // Для обычных продуктов - меньшее количество
                quantity = 1 + random.nextInt(4); // Количество от 1 до 4
            }
            
            Purchase purchase = new Purchase(
                purchaseId,
                productId,
                quantity
            );
            
            purchaseProducer.send(new ProducerRecord<>("purchases", purchaseId, purchase));
            System.out.println("Generated purchase: ID=" + purchaseId + 
                             ", ProductID=" + productId + 
                             ", Quantity=" + quantity);
            
            purchaseId++;
            
            // Пауза между покупками
            Thread.sleep(1000 + random.nextInt(2000)); // 1-3 секунды
            
            // Периодический отчет
            if (purchaseId % 10 == 0) {
                System.out.println("=== Generated " + purchaseId + " purchases ===");
            }
        }
    }
}