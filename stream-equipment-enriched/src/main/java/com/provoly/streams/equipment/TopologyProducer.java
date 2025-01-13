package com.provoly.streams.equipment;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import com.provoly.streams.equipment.dto.*;

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

        var equipmentHypervisorSerde = new ObjectMapperSerde<>(EquipmentHypervisor.class);
        var itemDtoSerde = new ObjectMapperSerde<>(ItemDto.class);
        var equipmentResultSerde = new JsonObjectSerde();

        KTable<String, EquipmentHypervisor> equipment = builder
                .stream(equipmentTopic, Consumed.with(Serdes.String(), equipmentHypervisorSerde))
                .toTable();

        Pattern measuresTopicName = Pattern.compile("class-([a-f0-9]{10})_.*-mesures");

        KGroupedStream<String, ItemDto> measures = builder
                .stream(measuresTopicName, Consumed.with(Serdes.String(), itemDtoSerde))
                .groupBy(((key, value) -> value.getSimple("reference")),
                        Grouped.with(Serdes.String(), itemDtoSerde));

        KTable<String, ItemDto> reducedMeasures = measures.reduce((aggValue, newValue) -> {
            aggValue.getAttributes().forEach((key, value) -> newValue.getAttributes().putIfAbsent(key, value));
            return newValue;
        });

        equipment.leftJoin(reducedMeasures, this::join)
                .toStream()
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
                                record.key().getBytes(StandardCharsets.UTF_8)));
                        context.forward(record);
                    }
                })
                .to(enrichedDatasetTopic, Produced.with(Serdes.String(), equipmentResultSerde));
        return builder.build();
    }

    private JsonObject join(EquipmentHypervisor eqt, ItemDto measures) {
        var result = new JsonObject();

        appendEquipmentPropertyToResult(eqt, "code", result);
        appendEquipmentPropertyToResult(eqt, "domain", result);
        appendEquipmentPropertyToResult(eqt, "entity", result);
        appendEquipmentPropertyToResult(eqt, "family", result);
        appendEquipmentPropertyToResult(eqt, "position", result);
        appendEquipmentPropertyToResult(eqt, "place", result);
        appendEquipmentPropertyToResult(eqt, "managed", result);
        appendEquipmentPropertyToResult(eqt, "deleted", result);
        appendEquipmentPropertyToResult(eqt, "nbServicesAskedInProgress", result);

        var categories = eqt.events().stream().map(EventHypervisor::category).collect(Collectors.joining(","));
        result.put("category", categories);

        var criticalities = eqt.events().stream().map(EventHypervisor::criticality).collect(Collectors.joining(","));
        result.put("criticality", criticalities);

        var serviceCategories = eqt.services().stream().map(ServiceHypervisor::category).collect(Collectors.joining(","));
        result.put("serviceCategory", serviceCategories);

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

    private void appendEquipmentPropertyToResult(EquipmentHypervisor eqt, String property, JsonObject result) {
        if (eqt.attributes().get(property) != null) {
            result.put(property, eqt.attributes().get(property));
        }
    }

    private String multiToString(AttributeMultiValueDto multi) {
        return multi.getValues().stream().map(Object::toString).collect(Collectors.joining(";"));
    }
}
