package com.provoly.streams.consumption;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import com.provoly.streams.consumption.dto.AttributeMultiValueDto;
import com.provoly.streams.consumption.dto.AttributeSimpleValueDto;
import com.provoly.streams.consumption.dto.ItemDto;

import io.quarkus.kafka.client.serialization.JsonObjectSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.vertx.core.json.JsonObject;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class TopologyProducer {

    // TODO : Correct parameters for bootstrap server
    // TODO : Correctly managed kubernetes restart (loose data)

    @ConfigProperty(name = "provoly.consumption_dataset_topic")
    String consumptionPerDayDatasetTopic;

    @ConfigProperty(name = "provoly.dataset_version_id")
    String datasetVersionId;

    @Produces
    public Topology topologyService() {
        StreamsBuilder builder = new StreamsBuilder();

        var itemDtoSerde = new ObjectMapperSerde<>(ItemDto.class);
        var equipmentResultSerde = new JsonObjectSerde();

        Pattern measuresTopicName = Pattern.compile("class-([a-f0-9]{10})_armoire-consommation-mesures");

        builder.stream(measuresTopicName, Consumed.with(Serdes.String(), itemDtoSerde))
                .map((key, value) -> KeyValue.pair(buildKey(value), mapToJsonObject(value)))
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
                .to(consumptionPerDayDatasetTopic, Produced.with(Serdes.String(), equipmentResultSerde));
        return builder.build();
    }

    private String buildKey(ItemDto value) {
        var date = Instant.parse(value.getSimple("measuredAt")).truncatedTo(ChronoUnit.DAYS);
        return "%s_%s".formatted(value.getSimple("reference"), date);
    }

    private JsonObject mapToJsonObject(ItemDto measures) {
        var result = new JsonObject();

        for (var measure : measures.getAttributes().entrySet()) {
            switch (measure.getValue()) {
                case AttributeSimpleValueDto simple -> result.put(measure.getKey(), simple.getValue());
                case AttributeMultiValueDto multi -> result.put(measure.getKey(), multiToString(multi));
                default -> throw new IllegalStateException("Unexpected value : " + measure.getValue());
            }
        }

        return result;
    }

    private String multiToString(AttributeMultiValueDto multi) {
        return multi.getValues().stream().map(Object::toString).collect(Collectors.joining(";"));
    }
}
