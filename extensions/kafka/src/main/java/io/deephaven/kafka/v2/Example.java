/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.processor.ObjectProcessor.strict;

public class Example {

    public static Table homeassistant() {
        return stringStringTable(
                Map.of("bootstrap.servers", "192.168.52.16:9092,192.168.52.17:9092,192.168.52.18:9092"),
                "homeassistant");
    }

    public static Table stringStringTable(Map<String, String> kafkaClientConfig, String topic) {
        final ConsumerRecordOptions basicOptions = ConsumerRecordOptions.of();
        final ObjectProcessor<ConsumerRecord<String, String>> processor =
                strict(ObjectProcessor.<ConsumerRecord<String, String>>combined(List.of(
                        strict(Processors.basic(basicOptions)),
                        strict(Processors.key(Type.stringType())),
                        strict(Processors.value(Type.stringType())))));

        final KafkaPublisherDriver<String, String> driver;
        {
            final ClientOptions<String, String> clientOptions = ClientOptions.<String, String>builder()
                    .putAllConfig(kafkaClientConfig)
                    .keyDeserializer(new StringDeserializer())
                    .valueDeserializer(new StringDeserializer())
                    .build();
            driver = KafkaPublisherDriver.of(clientOptions, processor, 1024);
        }

        final StreamToBlinkTableAdapter adapter;
        {
            final List<String> columnNames =
                    Stream.concat(basicOptions.columnNames(), Stream.of("Key", "Value")).collect(Collectors.toList());
            final TableDefinition tableDefinition = TableDefinition.from(columnNames, processor.outputTypes());

            adapter = new StreamToBlinkTableAdapter(tableDefinition, driver, null, "todo");
        }



        // don't start it yet driver.start();


        final KafkaPublisher<String, String> publisher = KafkaToolsNew.of(processor, columnNames);
        {

            final Collection<TopicPartition> topicPartitions = Set.of(new TopicPartition(topic, 0));
            publisher.start(clientOptions, topicPartitions);
        }
        return BlinkTableTools.blinkToAppendOnly(publisher.table());
    }
}
