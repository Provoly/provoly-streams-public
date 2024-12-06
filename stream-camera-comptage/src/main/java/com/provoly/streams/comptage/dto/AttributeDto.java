package com.provoly.streams.comptage.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = AttributeSimpleValueDto.class, name = "VALUE"),
        @JsonSubTypes.Type(value = AttributeMultiValueDto.class, name = "MULTI")
})
public class AttributeDto {
    protected AttributeType type;

    public AttributeDto(AttributeType type) {
        this.type = type;
    }

    public AttributeType getType() {
        return type;
    }

    public void setType(AttributeType type) {
        this.type = type;
    }
}
