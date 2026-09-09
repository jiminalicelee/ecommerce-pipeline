package com.pipeline.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pipeline.model.ClickEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.errors.SerializationException;

public class EventDeserializer implements Deserializer<ClickEvent> {
    private final ObjectMapper mapper;

    public EventDeserializer() {
        this.mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public ClickEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        // Convert `data` to ClickEvent using `mapper`.
        try {
            return mapper.readValue(data, ClickEvent.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing bytes to ClickEvent", e);
        }
    }
}