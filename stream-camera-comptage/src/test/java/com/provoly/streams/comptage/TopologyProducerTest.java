package com.provoly.streams.comptage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import jakarta.inject.Inject;

import com.provoly.streams.comptage.dto.ItemDto;

import io.quarkus.kafka.client.serialization.JsonObjectSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class TopologyProducerTest {
    @Inject
    TopologyProducer topologyProducer;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ItemDto> inputTopicMultiModeMeasures;
    private TestInputTopic<String, JsonObject> inputTopicEquipments;
    private TestOutputTopic<String, JsonObject> outputTopic;

    @BeforeEach
    public void setup() {
        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<ItemDto> itemDtoSerde = new ObjectMapperSerde<>(ItemDto.class);
        Serde<JsonObject> equipmentResultSerde = new JsonObjectSerde();

        testDriver = new TopologyTestDriver(topologyProducer.topologyService());

        // setup test topics
        inputTopicMultiModeMeasures = testDriver.createInputTopic("class-aaaaaaaaaa_multi-mode",
                stringSerde.serializer(), itemDtoSerde.serializer());
        inputTopicEquipments = testDriver.createInputTopic("equipment", stringSerde.serializer(),
                equipmentResultSerde.serializer());

        outputTopic = testDriver.createOutputTopic("comptage-enrichi-camera", stringSerde.deserializer(),
                equipmentResultSerde.deserializer());
    }

    @Test
    public void should_get_mode_measure_for_a_camera() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String reference = "camera";

        ItemDto item = buildItemDto(reference, date.toString(), "car", "17");

        var camera = JsonObject.of(
                "code", reference,
                "family", "VP_CAM",
                "events", List.of(),
                "services", List.of());

        // when
        inputTopicEquipments.pipeInput(reference, camera);
        inputTopicMultiModeMeasures.pipeInput(reference, item);

        // then
        var result = outputTopic.readRecord();

        var actualHeaderId = getHeaderIdFromRecord(result);
        var expectedHeaderId = "%s_%s_%s".formatted(reference, date.toString(), "car");

        assertThat(actualHeaderId).isEqualTo(expectedHeaderId);
        assertThat(result.getValue().getValue("measuredAt")).isEqualTo(date.toString());
        assertThat(result.getValue().getValue("category")).isEqualTo("car");
        assertThat(result.getValue().getValue("value")).isEqualTo("17");
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    public void should_get_all_mode_measures_for_a_camera() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String reference = "camera";

        ItemDto item = buildItemDto(reference, date.toString(), "car", "17");
        ItemDto item2 = buildItemDto(reference, date.toString(), "van", "20");

        var camera = JsonObject.of(
                "code", reference,
                "family", "VP_CAM",
                "events", List.of(),
                "services", List.of());

        // when
        inputTopicEquipments.pipeInput(reference, camera);
        inputTopicMultiModeMeasures.pipeInput(reference, item);
        inputTopicMultiModeMeasures.pipeInput(reference, item2);

        // then
        var results = outputTopic.readRecordsToList();

        assertThat(results).hasSize(2);

        var actualCarHeaderId = getHeaderIdFromRecord(results.getFirst());
        var expectedCarHeaderId = "%s_%s_%s".formatted(reference, date.toString(), "car");

        var actualVanHeaderId = getHeaderIdFromRecord(results.getLast());
        var expectedVanHeaderId = "%s_%s_%s".formatted(reference, date.toString(), "van");

        assertThat(actualCarHeaderId).isEqualTo(expectedCarHeaderId);
        assertThat(actualVanHeaderId).isEqualTo(expectedVanHeaderId);

        assertThat(results.getFirst().getValue().getValue("category")).isEqualTo("car");
        assertThat(results.getFirst().getValue().getValue("value")).isEqualTo("17");

        assertThat(results.getLast().getValue().getValue("category")).isEqualTo("van");
        assertThat(results.getLast().getValue().getValue("value")).isEqualTo("20");
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    public void should_get_mode_measures_for_two_cameras() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String cameraRef1 = "camera";
        String cameraRef2 = "camera2";

        ItemDto item = buildItemDto(cameraRef1, date.toString(), "car", "17");
        ItemDto item2 = buildItemDto(cameraRef2, date.toString(), "car", "20");

        var camera = JsonObject.of(
                "code", cameraRef1,
                "family", "VP_CAM",
                "events", List.of(),
                "services", List.of());

        var camera2 = JsonObject.of(
                "code", cameraRef2,
                "family", "VP_CAM",
                "events", List.of(),
                "services", List.of());

        // when
        inputTopicEquipments.pipeInput(cameraRef1, camera);
        inputTopicEquipments.pipeInput(cameraRef2, camera2);

        inputTopicMultiModeMeasures.pipeInput(cameraRef1, item);
        inputTopicMultiModeMeasures.pipeInput(cameraRef2, item2);

        // then
        var results = outputTopic.readRecordsToList();

        assertThat(results).hasSize(2);

        var actualFirstHeaderId = getHeaderIdFromRecord(results.getFirst());
        var expectedFirstHeaderId = "%s_%s_%s".formatted(cameraRef1, date.toString(), "car");

        var actualSecondHeaderId = getHeaderIdFromRecord(results.getLast());
        var expectedSecondHeaderId = "%s_%s_%s".formatted(cameraRef2, date.toString(), "car");

        assertThat(actualFirstHeaderId).isEqualTo(expectedFirstHeaderId);
        assertThat(actualSecondHeaderId).isEqualTo(expectedSecondHeaderId);

        assertThat(results.getFirst().getValue().getValue("category")).isEqualTo("car");
        assertThat(results.getFirst().getValue().getValue("value")).isEqualTo("17");

        assertThat(results.getLast().getValue().getValue("category")).isEqualTo("car");
        assertThat(results.getLast().getValue().getValue("value")).isEqualTo("20");
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    private static String getHeaderIdFromRecord(TestRecord<String, JsonObject> results) {
        return new String(results.getHeaders().lastHeader("provoly-item-id").value(), StandardCharsets.UTF_8);
    }

    private static ItemDto buildItemDto(String ref, String measuredAt, String category, String value) {
        ItemDto item = new ItemDto(UUID.randomUUID(), "%s@%S".formatted(UUID.randomUUID().toString(), ref));
        item.put("camera_code", ref);
        item.put("measuredAt", measuredAt);
        item.put("category", category);
        item.put("value", value);
        return item;
    }

}
