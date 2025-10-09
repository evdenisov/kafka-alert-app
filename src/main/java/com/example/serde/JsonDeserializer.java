package com.example.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> clazz;

    public static final String CONFIG_VALUE_CLASS = "value.deserializer.class";

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (configs.containsKey(CONFIG_VALUE_CLASS)) {
            try {
                clazz = (Class<T>) Class.forName((String) configs.get(CONFIG_VALUE_CLASS));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to configure JsonDeserializer. Class not found: " + configs.get(CONFIG_VALUE_CLASS), e);
            }
        }
    }

    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    public JsonDeserializer() {
        // Default constructor
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}