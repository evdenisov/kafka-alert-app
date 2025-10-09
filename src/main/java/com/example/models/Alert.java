package com.example.models;

import java.time.Instant;

public class Alert {
    private long productId;
    private double totalRevenue;
    private String message;
    private Instant timestamp;

    public Alert() {}

    public Alert(long productId, double totalRevenue, String message, Instant timestamp) {
        this.productId = productId;
        this.totalRevenue = totalRevenue;
        this.message = message;
        this.timestamp = timestamp;
    }

    // Getters and Setters
    public long getProductId() { return productId; }
    public void setProductId(long productId) { this.productId = productId; }

    public double getTotalRevenue() { return totalRevenue; }
    public void setTotalRevenue(double totalRevenue) { this.totalRevenue = totalRevenue; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "Alert{productId=" + productId + 
               ", totalRevenue=" + String.format("%.2f", totalRevenue) + 
               ", message='" + message + "'" +
               ", timestamp=" + timestamp + "}";
    }
}