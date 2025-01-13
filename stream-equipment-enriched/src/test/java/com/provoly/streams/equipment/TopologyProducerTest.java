package com.provoly.streams.equipment;

import com.provoly.streams.equipment.dto.ItemDto;
import io.quarkus.kafka.client.serialization.JsonObjectSerde;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
public class TopologyProducerTest {
    @Inject
    TopologyProducer topologyProducer;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ItemDto> inputTopicArmoireMeasures;
    private TestInputTopic<String, ItemDto> inputTopicCameraMeasures;
    private TestInputTopic<String, JsonObject> inputTopicEquipments;
    private TestOutputTopic<String, JsonObject> outputTopic;

    @BeforeEach
    public void setup() {
        Serde<String> stringSerde = new Serdes.StringSerde();
        Serde<ItemDto> itemDtoSerde = new ObjectMapperSerde<>(ItemDto.class);
        Serde<JsonObject> equipmentResultSerde = new JsonObjectSerde();

        testDriver = new TopologyTestDriver(topologyProducer.topologyService());

        // setup test topics
        inputTopicArmoireMeasures = testDriver.createInputTopic("class-aaaaaaaaaa_armoire-mesures", stringSerde.serializer(),
                itemDtoSerde.serializer());
        inputTopicCameraMeasures = testDriver.createInputTopic("class-aaaaaaaaaa_camera-mesures", stringSerde.serializer(),
                itemDtoSerde.serializer());
        inputTopicEquipments = testDriver.createInputTopic("equipment", stringSerde.serializer(),
                equipmentResultSerde.serializer());

        outputTopic = testDriver.createOutputTopic("equipement-enrichi-mesure", stringSerde.deserializer(),
                equipmentResultSerde.deserializer());
    }

    @Test
    public void should_enrich_armoire_equipment_with_its_measures() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String reference = "armoire1";

        ItemDto item = buildItemDto(reference, date.toString(), Map.of("consommation", "26"));

        var event = JsonObject.of("category", "LIMIT", "criticality", "MEDIUM");
        var event2 = JsonObject.of("category", "OUTOFORDER", "criticality", "HIGH");
        var service = JsonObject.of("category", "CURA");
        var service2 = JsonObject.of("category", "CURA");

        var armoire = JsonObject.of(
                "code", reference,
                "family", "ARMOIRE",
                "events", List.of(event, event2),
                "services", List.of(service, service2));

        // when
        inputTopicArmoireMeasures.pipeInput(reference, item);
        inputTopicEquipments.pipeInput(reference, armoire);

        // then
        var result = outputTopic.readKeyValue();

        assertThat(result.key).isEqualTo(reference);
        assertThat(result.value.getValue("measuredAt")).isEqualTo(date.toString());
        assertThat(result.value.getValue("consommation")).isEqualTo("26");
        assertThat(result.value.getValue("family")).isEqualTo("ARMOIRE");
        assertThat(result.value.getValue("category")).isEqualTo("LIMIT,OUTOFORDER");
        assertThat(result.value.getValue("criticality")).isEqualTo("MEDIUM,HIGH");
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    public void should_enrich_equipments_with_their_measures() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String referenceArmoire = "armoire1";
        String referenceCamera = "camera2";

        ItemDto measureArmoire = buildItemDto(referenceArmoire, date.toString(), Map.of("consommation", "26"));
        ItemDto measureCamera = buildItemDto(referenceCamera, date.plus(1, ChronoUnit.DAYS).toString(), Map.of("consommation", "99"));

        var armoire = JsonObject.of(
                "code", referenceArmoire,
                "family", "ARMOIRE",
                "events", List.of(),
                "services", List.of());

        var camera = JsonObject.of(
                "code", referenceCamera,
                "family", "CAMERA",
                "events", List.of(),
                "services", List.of());

        // when
        inputTopicEquipments.pipeInput(referenceArmoire, armoire);
        inputTopicEquipments.pipeInput(referenceCamera, camera);

        inputTopicArmoireMeasures.pipeInput(referenceArmoire, measureArmoire);
        inputTopicCameraMeasures.pipeInput(referenceCamera, measureCamera);

        // then
        var result = outputTopic.readKeyValuesToMap();

        assertThat(result).hasSize(2);

        var enrichedArmoire = result.get(referenceArmoire);
        var enrichedCamera = result.get(referenceCamera);

        assertThat(enrichedArmoire.getValue("measuredAt")).isEqualTo(date.toString());
        assertThat(enrichedArmoire.getValue("consommation")).isEqualTo("26");
        assertThat(enrichedArmoire.getValue("family")).isEqualTo("ARMOIRE");

        assertThat(enrichedCamera.getValue("measuredAt")).isEqualTo(date.plus(1, ChronoUnit.DAYS).toString());
        assertThat(enrichedCamera.getValue("consommation")).isEqualTo("99");
        assertThat(enrichedCamera.getValue("family")).isEqualTo("CAMERA");
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    public void should_override_enriched_equipment_with_new_measure() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String referenceArmoire = "armoire1";

        ItemDto measureArmoire = buildItemDto(referenceArmoire, date.toString(), Map.of("consommation", "26"));
        ItemDto newMeasureArmoire = buildItemDto(referenceArmoire, date.plus(1, ChronoUnit.MINUTES).toString(), Map.of("consommation", "28"));

        var armoire = JsonObject.of(
                "code", referenceArmoire,
                "family", "ARMOIRE",
                "events", List.of(),
                "services", List.of());

        // when
        inputTopicEquipments.pipeInput(referenceArmoire, armoire);

        inputTopicArmoireMeasures.pipeInput(referenceArmoire, measureArmoire);
        inputTopicArmoireMeasures.pipeInput(referenceArmoire, newMeasureArmoire);

        // then
        var result = outputTopic.readKeyValuesToMap();

        assertThat(result).hasSize(1);

        var enrichedArmoire = result.get(referenceArmoire);

        assertThat(enrichedArmoire.getValue("measuredAt")).isEqualTo(date.plus(1, ChronoUnit.MINUTES).toString());
        assertThat(enrichedArmoire.getValue("consommation")).isEqualTo("28");
        assertThat(enrichedArmoire.getValue("family")).isEqualTo("ARMOIRE");

        assertThat(outputTopic.isEmpty()).isTrue();
    }


    @Test
    public void should_merge_all_equipment_measures() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String referenceArmoire = "armoire1";

        ItemDto measureArmoire = buildItemDto(referenceArmoire, date.toString(), Map.of("consommation", "28"));
        ItemDto newMeasureArmoire = buildItemDto(referenceArmoire, date.plus(1, ChronoUnit.MINUTES).toString(), Map.of("etat", "ok"));

        var armoire = JsonObject.of(
                "code", referenceArmoire,
                "family", "ARMOIRE",
                "events", List.of(),
                "services", List.of());

        // when
        inputTopicEquipments.pipeInput(referenceArmoire, armoire);

        inputTopicArmoireMeasures.pipeInput(referenceArmoire, measureArmoire);
        inputTopicArmoireMeasures.pipeInput(referenceArmoire, newMeasureArmoire);

        // then
        var result = outputTopic.readKeyValuesToMap();

        assertThat(result).hasSize(1);

        var enrichedArmoire = result.get(referenceArmoire);

        assertThat(enrichedArmoire.getValue("measuredAt")).isEqualTo(date.plus(1, ChronoUnit.MINUTES).toString());
        assertThat(enrichedArmoire.getValue("consommation")).isEqualTo("28");
        assertThat(enrichedArmoire.getValue("etat")).isEqualTo("ok");
        assertThat(enrichedArmoire.getValue("family")).isEqualTo("ARMOIRE");

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    public void should_override_only_updated_equipment_measures() {
        // given
        var date = Instant.parse("2024-06-26T14:00:00Z");
        String referenceArmoire = "armoire1";

        ItemDto measureArmoire = buildItemDto(referenceArmoire, date.toString(), Map.of("consommation", "28"));
        ItemDto newMeasureArmoire = buildItemDto(referenceArmoire, date.plus(1, ChronoUnit.MINUTES).toString(), Map.of("etat", "ok"));
        ItemDto updatedMeasureArmoire = buildItemDto(referenceArmoire, date.plus(1, ChronoUnit.MINUTES).toString(), Map.of("etat", "KO"));

        var armoire = JsonObject.of(
                "code", referenceArmoire,
                "family", "ARMOIRE",
                "events", List.of(),
                "services", List.of());

        // when
        inputTopicEquipments.pipeInput(referenceArmoire, armoire);

        inputTopicArmoireMeasures.pipeInput(referenceArmoire, measureArmoire);
        inputTopicArmoireMeasures.pipeInput(referenceArmoire, newMeasureArmoire);
        inputTopicArmoireMeasures.pipeInput(referenceArmoire, updatedMeasureArmoire);

        // then
        var result = outputTopic.readKeyValuesToMap();

        assertThat(result).hasSize(1);

        var enrichedArmoire = result.get(referenceArmoire);

        assertThat(enrichedArmoire.getValue("measuredAt")).isEqualTo(date.plus(1, ChronoUnit.MINUTES).toString());
        assertThat(enrichedArmoire.getValue("consommation")).isEqualTo("28");
        assertThat(enrichedArmoire.getValue("etat")).isEqualTo("KO");
        assertThat(enrichedArmoire.getValue("family")).isEqualTo("ARMOIRE");

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    private static ItemDto buildItemDto(String ref, String measuredAt, Map<String, Object> measures) {
        ItemDto item = new ItemDto(UUID.randomUUID(), "%s@%S".formatted(UUID.randomUUID().toString(), ref));
        item.put("reference", ref);
        item.put("measuredAt", measuredAt);
        measures.forEach(item::put);
        return item;
    }

}
