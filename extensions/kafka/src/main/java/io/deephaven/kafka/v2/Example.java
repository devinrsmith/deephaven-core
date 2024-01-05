/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import java.util.List;

public class Example {

    public static Table homeassistant() {
        final ConsumerRecordOptions basicOptions = ConsumerRecordOptions.of();
        return KafkaToolsNew.blinkTable(TableOptions.<Void, String>builder()
                .name("homeassistant")
                .clientOptions(ClientOptions.<Void, String>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        .keyDeserializer(new VoidDeserializer())
                        .valueDeserializer(new StringDeserializer())
                        .build())
                .subscribeOptions(SubscribeOptions.beginning("homeassistant"))
                .processor(ObjectProcessor.combined(List.of(
                        Processors.basic(basicOptions),
                        Processors.value(Type.stringType()))))
                .addAllColumnNames(() -> basicOptions.columnNames().iterator())
                .addColumnNames("Value")
                .build());
    }
}
