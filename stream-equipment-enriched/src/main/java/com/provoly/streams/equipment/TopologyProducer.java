package com.provoly.streams.equipment;

import io.quarkus.kafka.client.serialization.JsonObjectSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

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
        var equipmentResultSerde = new JsonObjectSerde();//new ObjectMapperSerde<Map<String, Object>>(Map.class);

        KTable<String, EquipmentHypervisor> equipment = builder
                .stream("equipment", Consumed.with(Serdes.String(), equipmentHypervisorSerde))
                .toTable();

        KTable<String, ItemDto> measures = builder
                .stream("class-f746090e67_armoire-mesures", Consumed.with(Serdes.String(), itemDtoSerde))
                .map((key, value) -> KeyValue.pair((String) value.getSimple("reference"), value))
                .toTable(Materialized.with(Serdes.String(), itemDtoSerde));

        equipment.leftJoin(measures, this::join)
                .toStream()
                .to("equipment-enriched-measure", Produced.with(Serdes.String(), equipmentResultSerde));

        return builder.build();
    }

    private JsonObject join(EquipmentHypervisor eqt, ItemDto measure) {
        var result = new JsonObject();
        result.put("code", eqt.code());
        result.put("nbServices", eqt.nbServices());
        if (measure != null) {
            result.put("position", measure.getSimple("position"));
        }
        return result;
    }
}
