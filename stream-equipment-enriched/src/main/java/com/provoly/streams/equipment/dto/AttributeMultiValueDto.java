package com.provoly.streams.equipment.dto;

import java.util.ArrayList;
import java.util.Collection;

public class AttributeMultiValueDto extends AttributeDto {

    private final Collection<AttributeSimpleValueDto> values = new ArrayList<>();

    public AttributeMultiValueDto() {
        super(AttributeType.MULTI);
    }

    public Collection<AttributeSimpleValueDto> getValues() {
        return values;
    }
}
