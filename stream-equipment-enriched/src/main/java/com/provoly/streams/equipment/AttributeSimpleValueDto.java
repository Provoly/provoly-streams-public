package com.provoly.streams.equipment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include;

public class AttributeSimpleValueDto extends AttributeDto {

    @JsonInclude(Include.NON_NULL)
    public Map<String, Object> metadata = new HashMap<>();
    public Object value;
    @JsonInclude(value = Include.CUSTOM, valueFilter = BooleanTrueFilter.class)
    public boolean visible;

    @JsonCreator
    public AttributeSimpleValueDto(Object value) {
        this.type = AttributeType.VALUE;
        this.value = value;
    }

    private static class BooleanTrueFilter {
        @Override
        public boolean equals(Object obj) {
            var value = (Boolean) obj;
            return value;
        }
    }
}
