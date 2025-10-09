package com.example.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Purchase {
    private long id;
    private long quantity;
    private long productId;

    public Purchase() {}

    public Purchase(@JsonProperty("id") long id,
                    @JsonProperty("quantity") long quantity,
                    @JsonProperty("productid") long productId) {
        this.id = id;
        this.quantity = quantity;
        this.productId = productId;
    }

    // Getters and Setters
    public long getId() { return id; }
    public void setId(long id) { this.id = id; }

    public long getQuantity() { return quantity; }
    public void setQuantity(long quantity) { this.quantity = quantity; }

    public long getProductId() { return productId; }
    public void setProductId(long productId) { this.productId = productId; }

    @Override
    public String toString() {
        return "Purchase{id=" + id + ", quantity=" + quantity + ", productId=" + productId + "}";
    }
}