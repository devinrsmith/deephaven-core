/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit5.EngineExtensions;
import io.deephaven.engine.util.TableTools;
import io.deephaven.kafka.v2.TableOptions.OpinionatedClientOptions;
import io.deephaven.kafka.v2.TableOptions.OpinionatedRecordOptions;
import io.deephaven.kafka.v2.TopicExtension.Topic;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@ExtendWith({TopicExtension.class, EngineExtensions.class})
public abstract class TableOptionsSingleTopicTestBase {


    public abstract Map<String, Object> adminConfig();

    public abstract Map<String, Object> producerConfig();

    public abstract Map<String, String> clientConfig();

    @Topic
    public String topic;

    @BeforeEach
    void createTopic() throws ExecutionException, InterruptedException {
        try (final Admin admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topic, Optional.of(1), Optional.empty()))).all().get();
        }
    }

    @AfterEach
    void deleteTopic() {
        // We _could_ delete the topics, but the container should be deleted soon enough, and each method has its own
        // topic name
    }

    @Test
    public void stringKey() throws ExecutionException, InterruptedException {
        final String keyValue = "mykey";
        try (final KafkaProducer<String, Void> producer = producer(new StringSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, keyValue, null)).get();
        }
        keyTest(Type.stringType(), new StringDeserializer(),
                TableTools.newTable(TableTools.stringCol("Key", keyValue)));
    }

    @Test
    public void uuidKey() throws ExecutionException, InterruptedException {
        final UUID keyValue = UUID.randomUUID();
        try (final KafkaProducer<UUID, Void> producer = producer(new UUIDSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, keyValue, null)).get();
        }
        keyTest(Type.ofCustom(UUID.class), new UUIDDeserializer(),
                TableTools.newTable(TableTools.col("Key", keyValue)));
    }

    @Test
    public void byteArrayKey() throws ExecutionException, InterruptedException {
        final byte[] keyValue = "hello, world!".getBytes(StandardCharsets.UTF_8);
        try (final KafkaProducer<byte[], Void> producer = producer(new ByteArraySerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, keyValue, null)).get();
        }
        keyTest(Type.byteType().arrayType(), new ByteArrayDeserializer(),
                TableTools.newTable(TableTools.col("Key", keyValue)));
    }

    @Disabled
    @Test
    public void intKey() throws ExecutionException, InterruptedException {
        final Integer keyValue = 42;
        try (final KafkaProducer<Integer, Void> producer = producer(new IntegerSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, keyValue, null)).get();
        }
        keyTest(Type.intType().boxedType(), new IntegerDeserializer(),
                TableTools.newTable(TableTools.intCol("Key", keyValue)));
    }


    private <K> void keyTest(GenericType<K> keyType, Deserializer<K> keyDeserializer, Table expected)
            throws InterruptedException {
        try (final StreamToBlinkTableAdapter adapter = TableOptions.<K, Void>builder()
                .clientOptions(clientOptions(keyDeserializer, new VoidDeserializer()))
                .opinionatedClientOptions(OpinionatedClientOptions.none())
                .addOffsets(Offsets.beginning(topic))
                .receiveTimestamp(null)
                .recordOptions(ConsumerRecordOptions.empty())
                .opinionatedRecordOptions(OpinionatedRecordOptions.none())
                .keyProcessor(ObjectProcessor.simple(keyType))
                // .callback(firstPollCompleted)
                .build()
                .adapter()) {
            // TODO: fix this sleep
            Thread.sleep(1000);
            // firstPollCompleted.await();
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(adapter::run);
            TstUtils.assertTableEquals(expected, adapter.table());
        }
    }

    final Admin admin() {
        return Admin.create(adminConfig());
    }

    final <K, V> KafkaProducer<K, V> producer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new KafkaProducer<>(producerConfig(), keySerializer, valueSerializer);
    }

    private <K, V> ClientOptions<K, V> clientOptions(Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        return ClientOptions.<K, V>builder()
                .putAllConfig(clientConfig())
                .keyDeserializer(keyDeserializer)
                .valueDeserializer(valueDeserializer)
                .build();
    }


    // static class FirstPollCompleted implements ConsumerLoopCallback {
    // private final CountDownLatch afterPollLatch = new CountDownLatch(1);
    //
    // public void await() throws InterruptedException {
    // afterPollLatch.await();
    // }
    //
    // @Override
    // public void beforePoll(KafkaConsumer<?, ?> consumer) {
    //
    // }
    //
    // @Override
    // public void afterPoll(KafkaConsumer<?, ?> consumer, boolean more) {
    // afterPollLatch.countDown();
    // }
    // }
}
