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
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RevenueAlertApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "revenue-alert-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        final Topology topology = buildTopology();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
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

        // Read from product topic and create GlobalKTable for product information
        GlobalKTable<Long, Product> productTable = builder.globalTable(
            "products",
            Consumed.with(Serdes.Long(), productSerde)
        );

        // Join purchase stream with product table to get product price
        KStream<Long, Double> purchaseRevenueStream = purchaseStream
            .selectKey((key, purchase) -> purchase.getProductId())
            .join(
                productTable,
                (productId, purchase) -> productId,
                (purchase, product) -> {
                    // Calculate revenue for this purchase
                    return purchase.getQuantity() * product.getPrice();
                }
            );

        // Group by product ID and create windowed stream for last minute
        KTable<Windowed<Long>, Double> productRevenue = purchaseRevenueStream
            .groupByKey(Grouped.with(Serdes.Long(), Serdes.Double()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .aggregate(
                () -> 0.0,
                (productId, revenue, aggregate) -> aggregate + revenue,
                Materialized.<Long, Double>as(Stores.persistentWindowStore(
                    "revenue-store", 
                    Duration.ofDays(1), 
                    Duration.ofMinutes(1), 
                    false
                )).withValueSerde(Serdes.Double())
            );

        // Generate alerts when revenue exceeds 3000
        productRevenue
            .toStream()
            .filter((windowedProductId, totalRevenue) -> totalRevenue > 3000.0)
            .mapValues((windowedProductId, totalRevenue) -> {
                long productId = windowedProductId.key();
                return new Alert(
                    productId,
                    totalRevenue,
                    String.format("Alert: Product %d exceeded revenue threshold. Total: %.2f", 
                                 productId, totalRevenue),
                    Instant.now()
                );
            })
            .to("revenue-alerts", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class), alertSerde));

        return builder.build();
    }
}