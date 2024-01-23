/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.redpanda.RedpandaContainer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class MyTest {

    @Rule
    public RedpandaContainer container = new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v23.3.3");

    @Test
    public void name() throws ExecutionException, InterruptedException {
        try (final Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("test", Optional.empty(), Optional.empty()))).all().get();
        }
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers()),
                new StringSerializer(),
                new StringSerializer())) {
            producer.send(new ProducerRecord<>("test", "mykey", "myvalue")).get();
        }

//        final Table table = TableOptions.<String, String>builder()
//                .clientOptions(ClientOptions.<String, String>builder()
//                        .putConfig(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers())
//                        .keyDeserializer(new StringDeserializer())
//                        .valueDeserializer(new StringDeserializer())
//                        .build())
//                .addOffsets(Offsets.beginning("test"))
//                .keyProcessor(ObjectProcessor.simple(Type.stringType()))
//                .valueProcessor(ObjectProcessor.simple(Type.stringType()))
//                .build()
//                .table();
    }
}
