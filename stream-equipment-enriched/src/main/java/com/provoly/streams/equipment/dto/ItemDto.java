package com.provoly.streams.equipment.dto;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;

public class ItemDto {

    private final String id; // Format : DatasetVersionUUID@id
    private final UUID oClass;
    private final UUID datasetVersionId;
    private final Map<String, AttributeDto> attributes = new HashMap<>();

    @JsonCreator
    public ItemDto(UUID oClass, String id, UUID datasetVersionId) {
        this.id = id;
        this.oClass = oClass;
        this.datasetVersionId = datasetVersionId;
    }

    public void put(String name, Object value) {
        attributes.put(name, new AttributeSimpleValueDto(value));
    }

    public <T> T getSimple(String attributeName) {
        var attribute = (AttributeSimpleValueDto) attributes.get(attributeName);
        if (attribute == null) {
            return null;
        }
        return (T) attribute.getValue();
    }

    public Map<String, AttributeDto> getAttributes() {
        return attributes;
    }

    public String getId() {
        return id;
    }

    public UUID getoClass() {
        return oClass;
    }

    public UUID getDatasetVersionId() {
        return datasetVersionId;
    }

    @Override
    public String toString() {
        return "ItemDto{" +
                "id='" + id + '\'' +
                ", oClass=" + oClass +
                ", attributes=" + attributes +
                '}';
    }
}
