package com.provoly.streams.equipment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import jakarta.inject.Inject;

import io.quarkus.kafka.client.serialization.JsonObjectSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class TopologyProducerTest {
    @Inject
    TopologyProducer topologyProducer;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ItemDto> inputTopic;
    private TestOutputTopic<String, JsonObject> outputTopic;

    @BeforeEach
    public void setup() {
        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<ItemDto> itemDtoSerde = new ObjectMapperSerde<>(ItemDto.class);
        Serde<JsonObject> equipmentResultSerde = new JsonObjectSerde();

        testDriver = new TopologyTestDriver(topologyProducer.topologyService());

        // setup test topics
        inputTopic = testDriver.createInputTopic("class-aaaaaaaaaa_armoire-mesures", stringSerde.serializer(),
                itemDtoSerde.serializer());
        outputTopic = testDriver.createOutputTopic("consommation-journaliere-armoire", stringSerde.deserializer(),
                equipmentResultSerde.deserializer());
    }

    @Test
    public void should_convert_item_to_equipment_result() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String reference = "armoire1";

        ItemDto item = buildItemDto(reference, date.toString(), "26");

        // when
        inputTopic.pipeInput(null, item);

        // then
        String expectedOutputKey = "%s_%s".formatted(reference, date.truncatedTo(ChronoUnit.DAYS).toString());

        var result = outputTopic.readKeyValue();

        assertThat(result.key).isEqualTo(expectedOutputKey);
        assertThat(result.value.getValue("measuredAt")).isEqualTo(date.toString());
        assertThat(result.value.getValue("consommation")).isEqualTo("26");
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    public void should_override_last_equipment_result_with_new_consumption_of_the_day() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String reference = "armoire1";

        ItemDto item = buildItemDto(reference, date.toString(), "26");
        inputTopic.pipeInput(null, item);

        // when
        var newDate = Instant.parse("2024-06-26T16:00:00Z");

        ItemDto newItem = buildItemDto(reference, newDate.toString(), "99");
        inputTopic.pipeInput(null, newItem);

        // then
        var result = outputTopic.readKeyValuesToMap();

        String expectedOutputKey = "%s_%s".formatted(reference, date.truncatedTo(ChronoUnit.DAYS).toString());

        assertThat(result).hasSize(1);
        assertThat(result).containsKey(expectedOutputKey);
        assertThat(result.get(expectedOutputKey).getValue("consommation")).isEqualTo("99");
        assertThat(result.get(expectedOutputKey).getValue("measuredAt")).isEqualTo("2024-06-26T16:00:00Z");
    }

    @Test
    public void should_add_new_result_when_consommation_is_on_another_day() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String reference = "armoire1";

        ItemDto item = buildItemDto(reference, date.toString(), "26");
        inputTopic.pipeInput(null, item);

        // when
        var newDate = Instant.parse("2024-06-27T14:00:00Z");

        ItemDto newItem = buildItemDto(reference, newDate.toString(), "99");
        inputTopic.pipeInput(null, newItem);

        // then
        var result = outputTopic.readKeyValuesToMap();

        String outputKey1 = "%s_%s".formatted(reference, date.truncatedTo(ChronoUnit.DAYS).toString());
        String outputKey2 = "%s_%s".formatted(reference, newDate.truncatedTo(ChronoUnit.DAYS).toString());

        assertThat(result).hasSize(2);

        assertThat(result.get(outputKey1).getValue("consommation")).isEqualTo("26");
        assertThat(result.get(outputKey1).getValue("measuredAt")).isEqualTo("2024-06-26T14:00:00Z");

        assertThat(result.get(outputKey2).getValue("consommation")).isEqualTo("99");
        assertThat(result.get(outputKey2).getValue("measuredAt")).isEqualTo("2024-06-27T14:00:00Z");
    }

    private static ItemDto buildItemDto(String ref, String measuredAt, String consommation) {
        ItemDto item = new ItemDto(UUID.randomUUID(), "%s@%S".formatted(UUID.randomUUID().toString(), ref));
        item.put("reference", ref);
        item.put("measuredAt", measuredAt);
        item.put("consommation", consommation);
        return item;
    }

}
