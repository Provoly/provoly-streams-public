package com.provoly.streams.equipment;

import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.quarkus.kafka.client.serialization.JsonObjectSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.vertx.core.json.JsonObject;

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
    // TODO : Correct parameters for kafak topics
    // TODO : Correctly managed kubernetes restart (loose data)

    @Produces
    public Topology topologyService() {
        StreamsBuilder builder = new StreamsBuilder();

        var equipmentHypervisorSerde = new ObjectMapperSerde<>(EquipmentHypervisor.class);
        var itemDtoSerde = new ObjectMapperSerde<>(ItemDto.class);
        var equipmentResultSerde = new JsonObjectSerde();

        KTable<String, EquipmentHypervisor> equipment = builder
                .stream("equipment", Consumed.with(Serdes.String(), equipmentHypervisorSerde))
                .toTable();

        Pattern equipmentTopicName = Pattern.compile("class-([a-f0-9]{10})_.*-mesures");

        KTable<String, ItemDto> measures = builder
                .stream(equipmentTopicName, Consumed.with(Serdes.String(), itemDtoSerde))
                .map((key, value) -> KeyValue.pair((String) value.getSimple("reference"), value))
                .toTable(Materialized.with(Serdes.String(), itemDtoSerde));

        equipment.leftJoin(measures, this::join)
                .toStream()
                .to("equipment-enriched-measure", Produced.with(Serdes.String(), equipmentResultSerde));

        return builder.build();
    }

    private JsonObject join(EquipmentHypervisor eqt, ItemDto measures) {
        var result = new JsonObject();
        result.put("code", eqt.code());
        result.put("nbServices", eqt.nbServices());
        if (measures != null) {
            for (var measure : measures.getAttributes().entrySet()) {
                switch (measure.getValue()) {
                    case AttributeSimpleValueDto simple -> result.put(measure.getKey(), simple.value);
                    case AttributeMultiValueDto multi -> result.put(measure.getKey(), multiToString(multi));
                    default -> throw new IllegalStateException("Unexpected value: " + measure.getValue());
                }
            }
        }
        return result;
    }

    private String multiToString(AttributeMultiValueDto multi) {
        return multi.values.stream().map(Object::toString).collect(Collectors.joining(";"));
    }
}
