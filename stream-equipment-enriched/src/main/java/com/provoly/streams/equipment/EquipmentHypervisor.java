package com.provoly.streams.equipment;

import java.util.List;

public record EquipmentHypervisor(String code,
        String family,
        String domain,
        String entity,
        List<EventHypervisor> events) {

}
