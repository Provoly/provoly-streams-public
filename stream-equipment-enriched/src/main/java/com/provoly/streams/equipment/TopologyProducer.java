package com.provoly.streams.equipment;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import com.provoly.streams.equipment.dto.*;

import io.quarkus.kafka.client.serialization.JsonObjectSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.vertx.core.json.JsonObject;

import jakarta.inject.Inject;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

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

    @Inject
    Logger logger;

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

        KTable<String, ItemDto> reducedMeasures = measures.reduce(this::merge);

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
                        updateMeasureAt(record);
                        context.forward(record);
                    }
                })
                .to(enrichedDatasetTopic, Produced.with(Serdes.String(), equipmentResultSerde));
        return builder.build();
    }

    private static void updateMeasureAt(Record<String, JsonObject> record) {
        // Remove temporary "measuredAt-" attributes and get the last one to measuredAt
        JsonObject value = record.value();
        var keysToRemove = value.fieldNames().stream()
                .filter(key -> key.startsWith("measuredAt-"))
                .toList();
        var strLastDate = value.getString("measuredAt");

        if (strLastDate == null && keysToRemove.isEmpty()) {
            return; // No date at all, we do not add one
        }
        var lastDate = strLastDate==null?Instant.MIN:Instant.parse(strLastDate);

        for (var key : keysToRemove) {
            var date = Instant.parse(value.getString(key));
            if (date.isAfter(lastDate)) {
                lastDate = date;
            }
            value.remove(key);
        }
        value.put("measuredAt", lastDate.toString());
    }

    /**
     * Merge the two values. We kept the one with is most recent by datasetId
     * If there is no date, we kept
     * resulting stream.
     *
     * @param currentValue : The current value
     * @param newValue : The next value in the stream
     * @return : The merged value
     */
    private ItemDto merge(ItemDto currentValue, ItemDto newValue) {

        UUID datasetVersionId = newValue.getDatasetVersionId();
        var measureDateAttributeName = "measuredAt-" + datasetVersionId;

        var currentValueMeasureDate = getMeasureDateForDataSet(currentValue, datasetVersionId);
        var newValueMeasureDate = getMeasureDateForDataSet(newValue, datasetVersionId);

        if (newValueMeasureDate == null) { // No date in incoming measure
            logger.warn("No measuredAt for " + datasetVersionId);
            // If this warn never append, we can remove this and throw an exception
            return update(measureDateAttributeName, Instant.now(), currentValue, newValue);
        }

        if (currentValueMeasureDate == null) {
            return update(measureDateAttributeName, newValueMeasureDate, currentValue, newValue);
        }

        if (!newValueMeasureDate.isBefore(currentValueMeasureDate)) {
            // We updating even if the date is the same
            return update(measureDateAttributeName, newValueMeasureDate, currentValue, newValue);
        } else {
            // New value is after the current one, we ignoring the new value
            return currentValue;
        }

    }

    /**
     * Get the measure date for the dataset version
     *
     * @param item : The item to get the measure date from
     * @param datasetVersionId : The dataset version id
     * @return : The measure date for the dataset version
     */
    private Instant getMeasureDateForDataSet(ItemDto item, UUID datasetVersionId) {
        var measureDateAttributeName = "measuredAt-" + datasetVersionId;
        String measureDate = item.getSimple(measureDateAttributeName);
        if (measureDate == null) {
            // If no date, check if the current item is the one for the current dataset
            UUID itemDatasetId = item.getDatasetVersionId();
            if (itemDatasetId != null && itemDatasetId.equals(datasetVersionId)) {
                measureDate = item.getSimple("measuredAt");
                if (measureDate != null) {
                    return Instant.parse(measureDate);
                }
            }
            return null;
        }
        return Instant.parse(measureDate);
    }

    /**
     * /!\ This method is modifying the newValue object
     * @param measureDateAttributeName : The name of the attribute containing the date for the current dataset
     * @param currentValue : The current value
     * @param newValue : The new value
     * @return : The updated value (Reference to the newValue object updated)
     */
    private ItemDto update(String measureDateAttributeName, Instant newMeasureDate, ItemDto currentValue, ItemDto newValue) {
        currentValue.getAttributes().forEach((key, value) -> newValue.getAttributes().putIfAbsent(key, value));
        newValue.put(measureDateAttributeName, newMeasureDate);
        return newValue;

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
