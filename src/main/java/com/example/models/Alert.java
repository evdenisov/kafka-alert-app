package com.example.models;

import java.time.Instant;

public class Alert {
    private Long productId;
    private Double revenue;
    private String message;
    private Instant timestamp;

    // Конструкторы
    public Alert() {}

    // Конструктор с Instant
    public Alert(Long productId, Double revenue, String message, Instant timestamp) {
        this.productId = productId;
        this.revenue = revenue;
        this.message = message;
        this.timestamp = timestamp;
    }

    // Новый конструктор с long (миллисекунды)
    public Alert(Long productId, Double revenue, String message, long timestampMillis) {
        this.productId = productId;
        this.revenue = revenue;
        this.message = message;
        this.timestamp = Instant.ofEpochMilli(timestampMillis);
    }

    // Геттеры и сеттеры
    public Long getProductId() { return productId; }
    public void setProductId(Long productId) { this.productId = productId; }
    public Double getRevenue() { return revenue; }
    public void setRevenue(Double revenue) { this.revenue = revenue; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "Alert{productId=" + productId + ", revenue=" + revenue + 
               ", message='" + message + "', timestamp=" + timestamp + "}";
    }
}