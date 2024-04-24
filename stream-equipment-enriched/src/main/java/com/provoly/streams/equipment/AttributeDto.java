package com.provoly.streams.equipment;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = AttributeSimpleValueDto.class, name = "VALUE"),
        @JsonSubTypes.Type(value = AttributeMultiValueDto.class, name = "MULTI")
})
public class AttributeDto {
    public AttributeType type;

    public AttributeType getType() {
        return type;
    }
}
