/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit5.EngineExtensions;
import io.deephaven.engine.util.TableTools;
import io.deephaven.kafka.v2.ConsumerRecordOptions.Field;
import io.deephaven.kafka.v2.TableOptions.OpinionatedRecordOptions;
import io.deephaven.kafka.v2.TopicExtension.Topic;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith({TopicExtension.class, EngineExtensions.class})
public abstract class TableOptionsSingleTopicTestBase {

    public abstract Map<String, Object> adminConfig();

    public abstract Map<String, Object> producerConfig();

    public abstract Map<String, String> clientConfig();

    // 0 for Kafka, 1 for Redpanda
    public abstract int initialLeaderEpoch();

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
    void stringKey() throws ExecutionException, InterruptedException {
        final String key = "mykey";
        try (final KafkaProducer<String, Void> producer = producer(new StringSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.stringType(), new StringDeserializer(),
                TableTools.newTable(TableTools.stringCol("Key", key)));
    }

    @Test
    void uuidKey() throws ExecutionException, InterruptedException {
        final UUID key = UUID.randomUUID();
        try (final KafkaProducer<UUID, Void> producer = producer(new UUIDSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.ofCustom(UUID.class), new UUIDDeserializer(),
                TableTools.newTable(TableTools.col("Key", key)));
    }

    @Test
    void byteArrayKey() throws ExecutionException, InterruptedException {
        final byte[] key = "hello, world!".getBytes(StandardCharsets.UTF_8);
        try (final KafkaProducer<byte[], Void> producer = producer(new ByteArraySerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.byteType().arrayType(), new ByteArrayDeserializer(),
                TableTools.newTable(TableTools.col("Key", key)));
    }

    @Test
    void intKey() throws ExecutionException, InterruptedException {
        final Integer key = 42;
        try (final KafkaProducer<Integer, Void> producer = producer(new IntegerSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.intType().boxedType(), new IntegerDeserializer(),
                TableTools.newTable(TableTools.intCol("Key", key)));
    }

    @Test
    void stringValue() throws ExecutionException, InterruptedException {
        final String value = "myvalue";
        try (final KafkaProducer<Void, String> producer = producer(new VoidSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.stringType(), new StringDeserializer(),
                TableTools.newTable(TableTools.stringCol("Value", value)));
    }

    @Test
    void uuidValue() throws ExecutionException, InterruptedException {
        final UUID value = UUID.randomUUID();
        try (final KafkaProducer<Void, UUID> producer = producer(new VoidSerializer(), new UUIDSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.ofCustom(UUID.class), new UUIDDeserializer(),
                TableTools.newTable(TableTools.col("Value", value)));
    }

    @Test
    void byteArrayValue() throws ExecutionException, InterruptedException {
        final byte[] value = "hello, world!".getBytes(StandardCharsets.UTF_8);
        try (final KafkaProducer<Void, byte[]> producer = producer(new VoidSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.byteType().arrayType(), new ByteArrayDeserializer(),
                TableTools.newTable(TableTools.col("Value", value)));
    }

    @Test
    void intValue() throws ExecutionException, InterruptedException {
        final Integer value = 42;
        try (final KafkaProducer<Void, Integer> producer = producer(new VoidSerializer(), new IntegerSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.intType().boxedType(), new IntegerDeserializer(),
                TableTools.newTable(TableTools.intCol("Value", value)));
    }

    @Test
    void topicRecordOption() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        assertThat(metadata.topic()).isEqualTo(topic);
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.TOPIC).build(),
                TableTools.newTable(TableTools.stringCol(Field.TOPIC.recommendedName(), topic)));
    }

    @Test
    void partitionRecordOption() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.PARTITION).build(),
                TableTools.newTable(TableTools.intCol(Field.PARTITION.recommendedName(), metadata.partition())));
    }

    @Test
    void offsetRecordOption() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.OFFSET).build(),
                TableTools.newTable(TableTools.longCol(Field.OFFSET.recommendedName(), metadata.offset())));
    }

    @Test
    void leaderEpochRecordOption() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.LEADER_EPOCH).build(),
                TableTools.newTable(TableTools.intCol(Field.LEADER_EPOCH.recommendedName(), initialLeaderEpoch())));
    }

    @Test
    void timestampTypeRecordOption() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        // Not sure how to configure broker to use log append time
        // https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html#message-timestamp-type
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.TIMESTAMP_TYPE).build(),
                TableTools.newTable(TableTools.col(Field.TIMESTAMP_TYPE.recommendedName(), TimestampType.CREATE_TIME)));
    }

    @Test
    void timestampRecordOption() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, 42L, null, null, null)).get();
        }
        assertThat(metadata.timestamp()).isEqualTo(42L);
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.TIMESTAMP).build(),
                TableTools
                        .newTable(TableTools.instantCol(Field.TIMESTAMP.recommendedName(), Instant.ofEpochMilli(42L))));
    }

    @Test
    void keySizeRecordOptionNoKey() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        assertThat(metadata.serializedKeySize()).isEqualTo(-1);
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.KEY_SIZE).build(),
                TableTools.newTable(TableTools.intCol(Field.KEY_SIZE.recommendedName(), QueryConstants.NULL_INT)));
    }

    @Test
    void keySizeRecordOptionEmptyKey() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<byte[], Void> producer = producer(new ByteArraySerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, new byte[0], null)).get();
        }
        assertThat(metadata.serializedKeySize()).isEqualTo(0);
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.KEY_SIZE).build(),
                TableTools.newTable(TableTools.intCol(Field.KEY_SIZE.recommendedName(), 0)));
    }

    @Test
    void keySizeRecordOption() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<byte[], Void> producer = producer(new ByteArraySerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, new byte[42], null)).get();
        }
        assertThat(metadata.serializedKeySize()).isEqualTo(42);
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.KEY_SIZE).build(),
                TableTools.newTable(TableTools.intCol(Field.KEY_SIZE.recommendedName(), 42)));
    }

    @Test
    void valueSizeRecordOptionNoValue() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        assertThat(metadata.serializedValueSize()).isEqualTo(-1);
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.VALUE_SIZE).build(),
                TableTools.newTable(TableTools.intCol(Field.VALUE_SIZE.recommendedName(), QueryConstants.NULL_INT)));
    }

    @Test
    void valueSizeRecordOptionEmptyValue() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, byte[]> producer = producer(new VoidSerializer(), new ByteArraySerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, new byte[0])).get();
        }
        assertThat(metadata.serializedValueSize()).isEqualTo(0);
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.VALUE_SIZE).build(),
                TableTools.newTable(TableTools.intCol(Field.VALUE_SIZE.recommendedName(), 0)));
    }

    @Test
    void valueSizeRecordOption() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, byte[]> producer = producer(new VoidSerializer(), new ByteArraySerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, new byte[42])).get();
        }
        assertThat(metadata.serializedValueSize()).isEqualTo(42);
        recordOption(
                ConsumerRecordOptions.builder().addField(Field.VALUE_SIZE).build(),
                TableTools.newTable(TableTools.intCol(Field.VALUE_SIZE.recommendedName(), 42)));
    }

    @Test
    void customRecordProcessor() throws ExecutionException, InterruptedException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, 42L, null, null, List.of(
                    new RecordHeader("CustomKey1", new byte[] {42}),
                    new RecordHeader("CustomKey2", new byte[] {43}),
                    new RecordHeader("CustomKey1", new byte[] {44})))).get();
        }
        final Table expected = TableTools.newTable(
                TableTools.<byte[]>col("CustomKey1", new byte[] {44}),
                TableTools.intCol("NumHeaders", 3));
        final FirstPollCompleted firstPollCompleted = new FirstPollCompleted();
        try (final StreamToBlinkTableAdapter adapter = TableOptions.<byte[], byte[]>builder()
                .clientOptions(clientOptions(new ByteArrayDeserializer(), new ByteArrayDeserializer()))
                .addOffsets(Offsets.beginning(topic))
                .receiveTimestamp(null)
                .recordOptions(ConsumerRecordOptions.empty())
                .opinionatedRecordOptions(OpinionatedRecordOptions.none())
                .recordProcessor(NamedObjectProcessor.of(ObjectProcessor.combined(List.of(
                        Processors.lastHeader("CustomKey1",
                                ObjectProcessorFunctions.identity(Type.byteType().arrayType())),
                        NumHeaders.INSTANCE)), List.of("CustomKey1", "NumHeaders")))
                // .keyProcessor(ObjectProcessor.simple(keyType))
                // .callback(firstPollCompleted)
                .build()
                .adapter()) {
            firstPollCompleted.await();
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(adapter::run);
            TstUtils.assertTableEquals(expected, adapter.table());
        }
    }

    @Test
    void receiveTimestamp() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        final TableDefinition expectedDefinition =
                TableDefinition.of(ColumnDefinition.of("ReceiveTimestamp", Type.instantType()));
        final FirstPollCompleted firstPollCompleted = new FirstPollCompleted();
        try (final StreamToBlinkTableAdapter adapter = TableOptions.<byte[], byte[]>builder()
                .clientOptions(clientOptions(new ByteArrayDeserializer(), new ByteArrayDeserializer()))
                .addOffsets(Offsets.beginning(topic))
                .recordOptions(ConsumerRecordOptions.empty())
                .opinionatedRecordOptions(OpinionatedRecordOptions.none())
                // .keyProcessor(ObjectProcessor.simple(keyType))
                // .callback(firstPollCompleted)
                .build()
                .adapter()) {
            firstPollCompleted.await();
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(adapter::run);
            assertThat(adapter.table().getDefinition()).isEqualTo(expectedDefinition);
            assertThat(adapter.table().size()).isEqualTo(1);
        }
    }

    @Test
    void extraAttributes() throws ExecutionException, InterruptedException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        final FirstPollCompleted firstPollCompleted = new FirstPollCompleted();
        try (final StreamToBlinkTableAdapter adapter = TableOptions.<byte[], byte[]>builder()
                .clientOptions(clientOptions(new ByteArrayDeserializer(), new ByteArrayDeserializer()))
                .addOffsets(Offsets.beginning(topic))
                .recordOptions(ConsumerRecordOptions.empty())
                .opinionatedRecordOptions(OpinionatedRecordOptions.none())
                .putExtraAttributes("CustomAttribute", 42L)
                // .keyProcessor(ObjectProcessor.simple(keyType))
                // .callback(firstPollCompleted)
                .build()
                .adapter()) {
            firstPollCompleted.await();
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(adapter::run);
            assertThat(adapter.table().getAttribute("CustomAttribute")).isEqualTo(42L);
        }
    }

    private void recordOption(ConsumerRecordOptions recordOptions, Table expected) throws InterruptedException {
        final FirstPollCompleted firstPollCompleted = new FirstPollCompleted();
        try (final StreamToBlinkTableAdapter adapter = TableOptions.<byte[], byte[]>builder()
                .clientOptions(clientOptions(new ByteArrayDeserializer(), new ByteArrayDeserializer()))
                .addOffsets(Offsets.beginning(topic))
                .receiveTimestamp(null)
                .recordOptions(recordOptions)
                .opinionatedRecordOptions(OpinionatedRecordOptions.none())
                // .keyProcessor(ObjectProcessor.simple(keyType))
                // .callback(firstPollCompleted)
                .build()
                .adapter()) {
            firstPollCompleted.await();
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(adapter::run);
            TstUtils.assertTableEquals(expected, adapter.table());
        }
    }

    private <K> void keyTest(GenericType<K> keyType, Deserializer<K> keyDeserializer, Table expected)
            throws InterruptedException {
        final FirstPollCompleted firstPollCompleted = new FirstPollCompleted();
        try (final StreamToBlinkTableAdapter adapter = TableOptions.<K, Void>builder()
                .clientOptions(clientOptions(keyDeserializer, new VoidDeserializer()))
                .addOffsets(Offsets.beginning(topic))
                .receiveTimestamp(null)
                .recordOptions(ConsumerRecordOptions.empty())
                .opinionatedRecordOptions(OpinionatedRecordOptions.none())
                .keyProcessor(ObjectProcessorFunctions.identity(keyType))
                // .callback(firstPollCompleted)
                .build()
                .adapter()) {
            firstPollCompleted.await();
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(adapter::run);
            TstUtils.assertTableEquals(expected, adapter.table());
        }
    }

    private <V> void valueTest(GenericType<V> valueType, Deserializer<V> valueDeserializer, Table expected)
            throws InterruptedException {
        final FirstPollCompleted firstPollCompleted = new FirstPollCompleted();
        try (final StreamToBlinkTableAdapter adapter = TableOptions.<Void, V>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), valueDeserializer))
                .addOffsets(Offsets.beginning(topic))
                .receiveTimestamp(null)
                .recordOptions(ConsumerRecordOptions.empty())
                .opinionatedRecordOptions(OpinionatedRecordOptions.none())
                .valueProcessor(ObjectProcessorFunctions.identity(valueType))
                // .callback(firstPollCompleted)
                .build()
                .adapter()) {
            firstPollCompleted.await();
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


    // todo: can use ConsumerLoopCallback-like structure?
    private static class FirstPollCompleted {

        public void await() throws InterruptedException {
            // TODO: fix this sleep
            Thread.sleep(1000);
        }
    }

    private enum NumHeaders implements ObjectProcessor<ConsumerRecord<?, ?>> {
        INSTANCE;

        @Override
        public List<Type<?>> outputTypes() {
            return List.of(Type.intType());
        }

        @Override
        public void processAll(ObjectChunk<? extends ConsumerRecord<?, ?>, ?> in, List<WritableChunk<?>> out) {
            for (int i = 0; i < in.size(); ++i) {
                int numHeaders = 0;
                final Iterator<Header> it = in.get(i).headers().iterator();
                while (it.hasNext()) {
                    it.next();
                    ++numHeaders;
                }
                out.get(0).asWritableIntChunk().add(numHeaders);
            }
        }
    }
}
