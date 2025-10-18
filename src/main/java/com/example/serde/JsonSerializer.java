package com.example.serde;

import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonSerializer() {
        // Регистрируем модуль для поддержки Java 8 date/time
        objectMapper.registerModule(new JavaTimeModule());
        // Отключаем запись дат как timestamp
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            System.err.println("Error serializing JSON: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Error serializing JSON", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}