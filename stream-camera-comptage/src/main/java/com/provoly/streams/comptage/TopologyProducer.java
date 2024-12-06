package com.provoly.streams.comptage;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import com.provoly.streams.comptage.dto.AttributeMultiValueDto;
import com.provoly.streams.comptage.dto.AttributeSimpleValueDto;
import com.provoly.streams.comptage.dto.EquipmentHypervisorDto;
import com.provoly.streams.comptage.dto.ItemDto;

import io.quarkus.kafka.client.serialization.JsonObjectSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.vertx.core.json.JsonObject;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class TopologyProducer {

    // TODO : Correct parameters for bootstrap server
    // TODO : Correctly managed kubernetes restart (loose data)

    @ConfigProperty(name = "provoly.enriched_dataset_topic")
    String enrichedDatasetTopic;

    @ConfigProperty(name = "quarkus.kafka-streams.topics")
    String equipmentTopic;

    @ConfigProperty(name = "provoly.dataset_version_id")
    String datasetVersionId;

    @Produces
    public Topology topologyService() {
        StreamsBuilder builder = new StreamsBuilder();

        var equipmentHypervisorSerde = new ObjectMapperSerde<>(EquipmentHypervisorDto.class);
        var itemDtoSerde = new ObjectMapperSerde<>(ItemDto.class);
        var equipmentResultSerde = new JsonObjectSerde();

        KTable<String, EquipmentHypervisorDto> equipment = builder
                .stream(equipmentTopic, Consumed.with(Serdes.String(), equipmentHypervisorSerde))
                .filter((k, v) -> v.attributes().get("family").equals("VP_CAM"))
                .toTable(Materialized.with(Serdes.String(), equipmentHypervisorSerde));

        Pattern measuresTopicName = Pattern.compile("class-([a-f0-9]{10})_multi-mode-mesures");

        KStream<String, ItemDto> measures = builder
                .stream(measuresTopicName, Consumed.with(Serdes.String(), itemDtoSerde))
                .selectKey((key, value) -> value.getSimple("camera_code").toString());

        measures.leftJoin(equipment, this::join, Joined.with(Serdes.String(), itemDtoSerde, equipmentHypervisorSerde))
                .process(() -> new Processor<String, JsonObject, String, JsonObject>() {
                    private ProcessorContext<String, JsonObject> context;

                    @Override
                    public void init(ProcessorContext<String, JsonObject> context) {
                        this.context = context;
                    }

                    @Override
                    public void process(Record<String, JsonObject> record) {
                        record.headers().add(new RecordHeader("provoly-dataset-version-id",
                                datasetVersionId.getBytes(StandardCharsets.UTF_8)));
                        record.headers().add(new RecordHeader("provoly-item-id",
                                buildKey(record.value()).getBytes(StandardCharsets.UTF_8)));
                        context.forward(record);
                    }
                })
                .to(enrichedDatasetTopic, Produced.with(Serdes.String(), equipmentResultSerde));
        return builder.build();
    }

    private String buildKey(JsonObject value) {
        var date = Instant.parse(value.getString("measuredAt"));
        return "%s_%s_%s".formatted(value.getString("camera_code"), date, value.getString("category"));
    }

    private JsonObject join(ItemDto measures, EquipmentHypervisorDto eqt) {
        var result = new JsonObject();

        appendEquipmentPropertyToResult(eqt, "code", result);
        appendEquipmentPropertyToResult(eqt, "entity", result);
        appendEquipmentPropertyToResult(eqt, "place", result);

        if (measures != null) {
            for (var measure : measures.getAttributes().entrySet()) {
                switch (measure.getValue()) {
                    case AttributeSimpleValueDto simple -> result.put(measure.getKey(), simple.getValue());
                    case AttributeMultiValueDto multi -> result.put(measure.getKey(), multiToString(multi));
                    default -> throw new IllegalStateException("Unexpected value: " + measure.getValue());
                }
            }
        }
        return result;
    }

    private void appendEquipmentPropertyToResult(EquipmentHypervisorDto eqt, String property, JsonObject result) {
        if (eqt.attributes().get(property) != null) {
            result.put(property, eqt.attributes().get(property));
        }
    }

    private String multiToString(AttributeMultiValueDto multi) {
        return multi.getValues().stream().map(Object::toString).collect(Collectors.joining(";"));
    }
}
