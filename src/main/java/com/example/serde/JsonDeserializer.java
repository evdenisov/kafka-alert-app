package com.example.serde;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> targetClass;

    public static final String CONFIG_VALUE_CLASS = "config.value.class";

    public JsonDeserializer() {
        // Регистрируем модуль для поддержки Java 8 date/time
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public JsonDeserializer(Class<T> targetClass) {
        this();
        this.targetClass = targetClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (targetClass == null) {
            String className = (String) configs.get(CONFIG_VALUE_CLASS);
            try {
                targetClass = (Class<T>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Target class not found: " + className, e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, targetClass);
        } catch (Exception e) {
            System.err.println("Error deserializing JSON: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Error deserializing JSON", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}