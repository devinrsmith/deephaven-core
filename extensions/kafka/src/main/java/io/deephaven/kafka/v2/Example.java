/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;

public class Example {

    public static Table homeassistant() {
        final ConsumerRecordOptions basicOptions = ConsumerRecordOptions.of();
        return KafkaTable.of(KafkaTableOptions.<Void, String>builder()
                .clientOptions(ClientOptions.<Void, String>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        .putConfig("group.id", "homeassistant-test-group-2")
                        .putConfig("auto.offset.reset", OffsetResetStrategy.NONE.toString())
                        .valueDeserializer(new StringDeserializer())
                        .build())
                .offsets(Offsets.committed("homeassistant", Offset.of(0)))
                .processor(ObjectProcessor.combined(List.of(
                        Processors.basic(basicOptions),
                        Processors.value(Type.stringType()))))
                .addAllColumnNames(() -> basicOptions.columnNames().iterator())
                .addColumnNames("Value")
                .build());
    }
}
