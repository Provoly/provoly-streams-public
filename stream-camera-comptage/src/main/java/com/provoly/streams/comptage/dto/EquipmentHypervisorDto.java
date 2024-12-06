package com.provoly.streams.comptage.dto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

public record EquipmentHypervisorDto(Map<String, Object> attributes,
        List<EventHypervisorDto> events, List<ServiceHypervisorDto> services) {

    public EquipmentHypervisorDto {
        if (attributes == null) {
            attributes = new HashMap<>();
        }
    }

    @JsonAnyGetter
    public Map<String, Object> attributes() {
        return attributes;
    }

    @JsonAnySetter
    public void setAttributes(String name, Object value) {
        attributes.put(name, value);
    }
}
