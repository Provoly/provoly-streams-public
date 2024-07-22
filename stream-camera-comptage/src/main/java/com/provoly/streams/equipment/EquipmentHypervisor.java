package com.provoly.streams.equipment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

public record EquipmentHypervisor(Map<String, Object> attributes,
        List<EventHypervisor> events, List<ServiceHypervisor> services) {

    public EquipmentHypervisor {
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
