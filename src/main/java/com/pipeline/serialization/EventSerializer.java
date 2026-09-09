package com.pipeline.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.model.ClickEvent;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class EventSerializer implements Serializer<ClickEvent> {
    private final ObjectMapper mapper;

    public EventSerializer() {
        this.mapper = JsonMapperFactory.createMapper();
    }

    @Override
    public byte[] serialize(String topic, ClickEvent event) {
        if (event == null) {
            return null;
        }
        // Convert `event` to byte[] using `mapper`.
        try {
            return mapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing Event to JSON", e);
        }
    }
}