package com.provoly.streams.equipment.dto;

import static com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;

public class AttributeSimpleValueDto extends AttributeDto {

    @JsonInclude(Include.NON_NULL)
    private final Object value;

    @JsonCreator
    public AttributeSimpleValueDto(Object value) {
        super(AttributeType.VALUE);
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}
