/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.kafka.KafkaTools.TableType;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;

public class Example {

    public static Table homeassistant() {
        return Tablez.of(homeassistantOptions());
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

    private static TableOptions<Void, String> homeassistantOptions() {
        return TableOptions.<Void, String>builder()
                .recordOptions(ConsumerRecordOptions.v1())
                .clientOptions(ClientOptions.<Void, String>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        // .putConfig("group.id", "homeassistant-test-group-2")
                        // .putConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString())
                        // .putConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString())
                        .valueDeserializer(new StringDeserializer())
                        .build())
                // .offsets(Offsets.committed("homeassistant"))
                .offsets(Offsets.timestamp("homeassistant", Duration.ofHours(4)))
                .valueProcessor(NamedObjectProcessor.of(ObjectProcessor.simple(Type.stringType()), "Value"))
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
