/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.kafka.KafkaTools.TableType;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;

public class Example {

    public static PartitionedTable homeassistant() {
        final ConsumerRecordOptions basicOptions = ConsumerRecordOptions.of();
        return Tablez.ofPartitioned(TableOptions.<Void, String>builder()
                .clientOptions(ClientOptions.<Void, String>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        .putConfig("group.id", "homeassistant-test-group-2")
                        .putConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString())
                        .valueDeserializer(new StringDeserializer())
                        .build())
                .offsets(Offsets.committed("homeassistant"))
                .processor(ObjectProcessor.combined(List.of(
                        Processors.basic(basicOptions),
                        Processors.value(Type.stringType()))))
                .addAllColumnNames(() -> basicOptions.columnNames().iterator())
                .addColumnNames("Value")
                .build());
    }

    public static PartitionedTable netdataMetrics() {
        // {"labels":{"__name__":"netdata_system_cpu_percentage_average","chart":"system.cpu","dimension":"user","family":"cpu","instance":"felian"},"name":"netdata_system_cpu_percentage_average","timestamp":"2024-01-11T00:47:36Z","value":"17.371059810000002"}
        final ConsumerRecordOptions basicOptions = ConsumerRecordOptions.of();
        return Tablez.ofPartitioned(TableOptions.<Void, String>builder()
                .clientOptions(ClientOptions.<Void, String>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        .valueDeserializer(new StringDeserializer())
                        .build())
                .offsets(Offsets.end("netdata-metrics"))
                .filter(cr -> cr.value().startsWith(
                        "{\"labels\":{\"__name__\":\"netdata_system_cpu_percentage_average\",\"chart\":\"system.cpu\",\"dimension\":\"user"))
                .processor(ObjectProcessor.combined(List.of(
                        Processors.basic(basicOptions),
                        Processors.value(Type.stringType()))))
                .addAllColumnNames(() -> basicOptions.columnNames().iterator())
                .addColumnNames("Value")
                .tableType(TableType.append())
                .build());
    }
}
