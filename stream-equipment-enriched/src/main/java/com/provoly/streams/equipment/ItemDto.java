package com.provoly.streams.equipment;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;

public class ItemDto {

    private final String id; // Format : DatasetVersionUUID@id
    private final UUID oClass;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> metadata = null;
    private Map<String, AttributeDto> attributes = new HashMap<>();

    @JsonCreator
    public ItemDto(UUID oClass, String id) {
        this.id = id;
        this.oClass = oClass;
    }

    public ItemDto(UUID oClass, UUID datasetId, String id, Map<String, AttributeDto> attributes) {
        this.id = datasetId + "@" + id;
        this.oClass = oClass;
        this.attributes = attributes;
    }

    public String getDatasetVersionId() {
        return this.id.split("@")[0];
    }

    public String getItemId() {
        return this.id.split("@")[1];
    }

    public String getId() {
        return id;
    }

    public UUID getoClass() {
        return oClass;
    }

    public void put(String name, Object value) {
        attributes.put(name, new AttributeSimpleValueDto(value));
    }

    public void putMetadata(String name, Object value) {
        getMetadata().put(name, value);
    }

    public <T> T getSimple(String attributeName) {
        var attribute = (AttributeSimpleValueDto) attributes.get(attributeName);
        return (T) attribute.value;
    }

    public Map<String, Object> getMetadata() {
        if (metadata == null) {
            metadata = new HashMap<>();
        }
        return metadata;
    }

    public AttributeSimpleValueDto getSimpleAttribute(String attributeName) {
        return (AttributeSimpleValueDto) attributes.get(attributeName);
    }

    public Map<String, AttributeDto> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "ItemDto{" +
                "id='" + id + '\'' +
                ", oClass=" + oClass +
                ", metadata=" + metadata +
                ", attributes=" + attributes +
                '}';
    }
}
