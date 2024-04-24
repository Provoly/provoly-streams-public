package com.provoly.streams.equipment;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@ApplicationScoped
public class TopologyProducer {

    // TODO : Correct parameters for bootstrap server
    // TODO : REad from all mesures
    // TODO : All correct serializers/de-serializers
    // TODO : Correctly managed kubernetes restart (loose data)

    @Produces
    public Topology topologyService() {
        StreamsBuilder builder = new StreamsBuilder();

        var equipmentHypervisorSerde = new ObjectMapperSerde<>(EquipmentHypervisor.class);
        var itemDtoSerde = new ObjectMapperSerde<>(ItemDto.class);

        KTable<String, EquipmentHypervisor> equipment = builder
                .stream("equipment", Consumed.with(Serdes.String(), equipmentHypervisorSerde))
                .toTable();

        KStream<String, ItemDto> measures = builder
                .stream("class-f746090e67_armoire-mesures", Consumed.with(Serdes.String(), itemDtoSerde))
                .map((key, value) -> KeyValue.pair(value.getSimple("reference"), value)) // TODO : FIXME Only for test
        ;

        var fff = Joined.with(Serdes.String(), itemDtoSerde, equipmentHypervisorSerde);
        measures.leftJoin(equipment, (measure, equip) -> " " + equip.code() + "/" + equip.nbServices() + " " + measure.getSimple("reference"), fff)
                .to("equipment-enriched-measure", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
