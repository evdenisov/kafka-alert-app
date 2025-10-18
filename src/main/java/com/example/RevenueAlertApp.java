package com.example;

import com.example.models.Product;
import com.example.models.Purchase;
import com.example.models.Alert;
import com.example.serde.JsonDeserializer;
import com.example.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RevenueAlertApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "revenue-alert-app-v2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        final Topology topology = buildTopology();
        System.out.println("Topology description:\n" + topology.describe());
        
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        streams.setStateListener((newState, oldState) -> {
            System.out.println("=== STATE CHANGE: " + oldState + " -> " + newState + " ===");
        });

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Shutting down streams application");
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println("Streams application started successfully");
            latch.await();
        } catch (Throwable e) {
            System.err.println("Error starting streams application: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create custom serde for Purchase
        JsonSerializer<Purchase> purchaseSerializer = new JsonSerializer<>();
        JsonDeserializer<Purchase> purchaseDeserializer = new JsonDeserializer<>(Purchase.class);
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseSerializer, purchaseDeserializer);

        // Create custom serde for Product
        JsonSerializer<Product> productSerializer = new JsonSerializer<>();
        JsonDeserializer<Product> productDeserializer = new JsonDeserializer<>(Product.class);
        Serde<Product> productSerde = Serdes.serdeFrom(productSerializer, productDeserializer);

        // Create custom serde for Alert
        JsonSerializer<Alert> alertSerializer = new JsonSerializer<>();
        JsonDeserializer<Alert> alertDeserializer = new JsonDeserializer<>(Alert.class);
        Serde<Alert> alertSerde = Serdes.serdeFrom(alertSerializer, alertDeserializer);

        // Read from purchase topic
        KStream<Long, Purchase> purchaseStream = builder.stream(
            "purchases", 
            Consumed.with(Serdes.Long(), purchaseSerde)
        );

        // Print purchases for debugging
        purchaseStream.foreach((key, purchase) -> {
            System.out.println("🛒 PURCHASE RECEIVED - ID: " + purchase.getId() + 
                             ", ProductID: " + purchase.getProductId() + 
                             ", Quantity: " + purchase.getQuantity());
        });

        // Read from product topic and create GlobalKTable for product information
        GlobalKTable<Long, Product> productTable = builder.globalTable(
            "products",
            Consumed.with(Serdes.Long(), productSerde)
        );

        // Join purchase stream with product table to get product price and calculate revenue
        KStream<Long, Double> purchaseRevenueStream = purchaseStream
            .selectKey((key, purchase) -> purchase.getProductId())
            .join(
                productTable,
                (productId, purchase) -> productId,
                (purchase, product) -> {
                    if (product == null) {
                        System.err.println("❌ No product found for ID: " + purchase.getProductId());
                        return 0.0;
                    }
                    double revenue = purchase.getQuantity() * product.getPrice();
                    System.out.println("💰 REVENUE CALCULATION - Product " + product.getId() + 
                                     ": " + purchase.getQuantity() + " × " + product.getPrice() + 
                                     " = " + revenue);
                    return revenue;
                }
            )
            .filter((productId, revenue) -> {
                boolean keep = revenue > 0.0;
                if (keep) {
                    System.out.println("✅ KEEPING REVENUE: " + revenue + " for product " + productId);
                }
                return keep;
            });

        // Group by product ID and create windowed stream for last 2 minutes
        KTable<Windowed<Long>, Double> productRevenue = purchaseRevenueStream
            .groupByKey(Grouped.with(Serdes.Long(), Serdes.Double()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(2)))
            .aggregate(
                () -> 0.0,
                (productId, revenue, aggregate) -> {
                    double newAggregate = aggregate + revenue;
                    System.out.println("📊 AGGREGATION - Product " + productId + 
                                     ": " + aggregate + " + " + revenue + 
                                     " = " + newAggregate);
                    return newAggregate;
                },
                Materialized.with(Serdes.Long(), Serdes.Double())
            );

        // Generate alerts when revenue exceeds 3000
        KStream<Long, Alert> alertStream = productRevenue
            .toStream()
            .peek((windowedProductId, totalRevenue) -> {
                System.out.println("🔍 CHECKING ALERT - Product " + windowedProductId.key() + 
                                 ": " + totalRevenue + " vs threshold 3000");
            })
            .filter((windowedProductId, totalRevenue) -> {
                boolean shouldAlert = totalRevenue > 3000.0;
                if (shouldAlert) {
                    System.out.println("🚨 ALERT CONDITION MET! Product " + windowedProductId.key() + 
                                     " has revenue " + totalRevenue + " > 3000");
                }
                return shouldAlert;
            })
            .map((windowedProductId, totalRevenue) -> {
                long productId = windowedProductId.key();
                Alert alert = new Alert(
                    productId,
                    totalRevenue,
                    String.format("Alert: Product %d exceeded revenue threshold. Total: %.2f", 
                                 productId, totalRevenue),
                    System.currentTimeMillis()
                );
                System.out.println("🎯 CREATING ALERT: " + alert.getMessage());
                return KeyValue.pair(productId, alert);
            });

        // Send alerts to topic
        alertStream.to("revenue-alerts", Produced.with(Serdes.Long(), alertSerde));

        return builder.build();
    }
}