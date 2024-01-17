/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.ObjectProcessorJsonValue;
import io.deephaven.json.StringOptions;
import io.deephaven.kafka.KafkaTools.TableType;
import io.deephaven.kafka.v2.ConsumerRecordOptions.Field;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

public class Example {

    public static Table homeassistant() {
        return Tablez.of(homeassistantOptions());
    }

    public static Table netdata() {
        return Tablez.of(netdataOptions());
    }

    public static PartitionedTable homeassistantPartitioned() {
        return Tablez.ofPartitioned(homeassistantOptions());
    }

    // public static PartitionedTable homeassistantPartitionedOld() {
    // final Properties properties = new Properties();
    // properties.put("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092");
    // return KafkaTools.consumeToPartitionedTable(
    // properties,
    // "homeassistant",
    // KafkaTools.ALL_PARTITIONS,
    // KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    // KafkaTools.Consume.ignoreSpec(),
    // KafkaTools.Consume.simpleSpec("Value", String.class),
    // TableType.append());
    // }

    private final static byte[] WINDSPEED = "{\"entity_id\": \"sensor.windspeed\"".getBytes(StandardCharsets.UTF_8);

    // private final static byte[] CPU =
    // "{\"labels\":{\"__name__\":\"netdata_system_cpu_percentage_average\",\"chart\":\"system.cpu\"".getBytes(StandardCharsets.UTF_8);

    private final static byte[] CPU = "{\"labels\":{\"__name__\":\"netdata_system_cpu_total\",\"chart\":\"system.cpu\""
            .getBytes(StandardCharsets.UTF_8);

    private static boolean startsWith(byte[] src, byte[] prefix) {
        if (src.length < prefix.length) {
            return false;
        }
        return Arrays.equals(src, 0, prefix.length, prefix, 0, prefix.length);
    }

    private static boolean isWindspeed(ConsumerRecord<?, byte[]> cr) {
        return startsWith(cr.value(), WINDSPEED);
    }

    private static boolean isCpu(ConsumerRecord<?, byte[]> cr) {
        return startsWith(cr.value(), CPU);
    }

    private static TableOptions<Void, byte[]> homeassistantOptions() {
        return TableOptions.<Void, byte[]>builder()
                .clientOptions(ClientOptions.<Void, byte[]>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        .valueDeserializer(new ByteArrayDeserializer())
                        .build())
                .useOpinionatedRecordOptions(false)
                .recordOptions(ConsumerRecordOptions.builder().addField(Field.OFFSET).build())
                // .receiveTimestamp(null)
                .offsets(Offsets.beginning("homeassistant"))
                .filter(Example::isWindspeed)
                .valueProcessor(new ObjectProcessorJsonValue(new JsonFactory(), ObjectOptions.builder()
                        .putFields("state", DoubleOptions.builder().allowString(true).build())
                        .putFields("last_changed", InstantOptions.standard())
                        .build()))
                .tableType(TableType.append())
                .build();
    }

    private static TableOptions<Void, byte[]> netdataOptions() {
        return TableOptions.<Void, byte[]>builder()
                .clientOptions(ClientOptions.<Void, byte[]>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        .valueDeserializer(new ByteArrayDeserializer())
                        .build())
                .offsets(Offsets.beginning("netdata-metrics"))
                .filter(Example::isCpu)
                .valueProcessor(new ObjectProcessorJsonValue(new JsonFactory(), ObjectOptions.builder()
                        .putFields("labels", ObjectOptions.builder()
                                .putFields("dimension", StringOptions.strict())
                                .build())
                        .putFields("timestamp", InstantOptions.strict())
                        .putFields("value", DoubleOptions.builder().allowString(true).build())
                        .build()))
                .receiveTimestamp(null)
                .useOpinionatedRecordOptions(false)
                .recordOptions(ConsumerRecordOptions.empty())
                .tableType(TableType.append())
                .build();
    }

    public static PartitionedTable netdataMetrics() {
        // {"labels":{"__name__":"netdata_system_cpu_percentage_average","chart":"system.cpu","dimension":"user","family":"cpu","instance":"felian"},"name":"netdata_system_cpu_percentage_average","timestamp":"2024-01-11T00:47:36Z","value":"17.371059810000002"}
        return Tablez.ofPartitioned(TableOptions.<Void, String>builder()
                .clientOptions(ClientOptions.<Void, String>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        .valueDeserializer(new StringDeserializer())
                        .build())
                .offsets(Offsets.end("netdata-metrics"))
                .filter(cr -> cr.value().startsWith(
                        "{\"labels\":{\"__name__\":\"netdata_system_cpu_percentage_average\",\"chart\":\"system.cpu\",\"dimension\":\"user"))
                .valueProcessor(NamedObjectProcessor.of(ObjectProcessor.simple(Type.stringType()), "Value"))
                .tableType(TableType.append())
                .build());
    }
}
