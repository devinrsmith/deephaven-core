/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.VoidDeserializer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Example {

    public static Table homeassistant() {



        final ConsumerRecordOptions basicOptions = ConsumerRecordOptions.of();
        final ObjectProcessor<ConsumerRecord<Void, String>> processor =
                ObjectProcessor.<ConsumerRecord<Void, String>>combined(List.of(
                        Processors.basic(basicOptions),
                        Processors.value(Type.stringType())));
        final List<String> columnNames =
                Stream.concat(basicOptions.columnNames(), Stream.of("Value")).collect(Collectors.toList());
        final Table blinkTable = KafkaToolsNew.blinkTable(
                "homeassistant",
                ExecutionContext.getContext().getUpdateGraph(),
                ClientOptions.<Void, String>builder()
                        .putConfig("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092")
                        .keyDeserializer(new VoidDeserializer())
                        .valueDeserializer(new StringDeserializer())
                        .build(),
                SubscribeOptions.beginning("homeassistant"),
                processor,
                columnNames,
                Map.of(),
                1024);
        return BlinkTableTools.blinkToAppendOnly(blinkTable);
    }
}
