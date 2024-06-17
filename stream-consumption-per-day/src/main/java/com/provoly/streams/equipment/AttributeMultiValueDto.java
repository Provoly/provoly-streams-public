package com.provoly.streams.equipment;

import java.util.ArrayList;
import java.util.Collection;

public class AttributeMultiValueDto extends AttributeDto {

    public Collection<AttributeSimpleValueDto> values = new ArrayList<>();

    public AttributeMultiValueDto() {
        this.type = AttributeType.MULTI;
    }
}
