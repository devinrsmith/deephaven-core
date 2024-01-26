/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkEquals;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.testutil.junit5.EngineExtensions;
import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.kafka.v2.PublishersOptions.Partitioning;
import io.deephaven.kafka.v2.TopicExtension.Topic;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.AssertionFailedError;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(OrderAnnotation.class)
@ExtendWith({TopicExtension.class, EngineExtensions.class})
public abstract class PublishersOptionsBase {

    public abstract Map<String, Object> adminConfig();

    public abstract Map<String, Object> producerConfig();

    public abstract Map<String, String> consumerConfig();

    @Topic
    public String topic;

    @BeforeEach
    void createTopic() throws ExecutionException, InterruptedException {
        createTopicImpl(topic, 1);
    }

    @AfterEach
    void deleteTopic() {
        // We _could_ delete the topics, but the container should be deleted soon enough, and each method has its own
        // topic name
    }

    @Order(0)
    @Test
    void init() {
        // forced to go first, accounts for shared container startup time
    }

    @Test
    void stringKey() throws ExecutionException, InterruptedException {
        final String key = "mykey";
        try (final KafkaProducer<String, Void> producer = producer(new StringSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.stringType(), new StringDeserializer(), ObjectChunk.chunkWrap(new String[] {key}));
    }

    @Test
    void uuidKey() throws ExecutionException, InterruptedException {
        final UUID key = UUID.randomUUID();
        try (final KafkaProducer<UUID, Void> producer = producer(new UUIDSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.ofCustom(UUID.class), new UUIDDeserializer(), ObjectChunk.chunkWrap(new UUID[] {key}));
    }

    @Test
    void byteArrayKey() throws ExecutionException, InterruptedException {
        final byte[] key = "hello, world!".getBytes(StandardCharsets.UTF_8);
        try (final KafkaProducer<byte[], Void> producer = producer(new ByteArraySerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.byteType().arrayType(), new ByteArrayDeserializer(), ObjectChunk.chunkWrap(new byte[][] {key}));
    }

    @Test
    void shortKey() throws ExecutionException, InterruptedException {
        final Short key = 42;
        try (final KafkaProducer<Short, Void> producer = producer(new ShortSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.shortType().boxedType(), new ShortDeserializer(), ShortChunk.chunkWrap(new short[] {key}));
    }

    @Test
    void intKey() throws ExecutionException, InterruptedException {
        final Integer key = 42;
        try (final KafkaProducer<Integer, Void> producer = producer(new IntegerSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.intType().boxedType(), new IntegerDeserializer(), IntChunk.chunkWrap(new int[] {key}));
    }

    @Test
    void longKey() throws ExecutionException, InterruptedException {
        final Long key = 42L;
        try (final KafkaProducer<Long, Void> producer = producer(new LongSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.longType().boxedType(), new LongDeserializer(), LongChunk.chunkWrap(new long[] {key}));
    }

    @Test
    void floatKey() throws ExecutionException, InterruptedException {
        final Float key = 42.0f;
        try (final KafkaProducer<Float, Void> producer = producer(new FloatSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.floatType().boxedType(), new FloatDeserializer(), FloatChunk.chunkWrap(new float[] {key}));
    }

    @Test
    void doubleKey() throws ExecutionException, InterruptedException {
        final Double key = 42.0d;
        try (final KafkaProducer<Double, Void> producer = producer(new DoubleSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.doubleType().boxedType(), new DoubleDeserializer(), DoubleChunk.chunkWrap(new double[] {key}));
    }

    @Test
    void stringValue() throws ExecutionException, InterruptedException {
        final String value = "myvalue";
        try (final KafkaProducer<Void, String> producer = producer(new VoidSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.stringType(), new StringDeserializer(), ObjectChunk.chunkWrap(new String[] {value}));
    }

    @Test
    void uuidValue() throws ExecutionException, InterruptedException {
        final UUID value = UUID.randomUUID();
        try (final KafkaProducer<Void, UUID> producer = producer(new VoidSerializer(), new UUIDSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.ofCustom(UUID.class), new UUIDDeserializer(), ObjectChunk.chunkWrap(new UUID[] {value}));
    }

    @Test
    void byteArrayValue() throws ExecutionException, InterruptedException {
        final byte[] value = "hello, world!".getBytes(StandardCharsets.UTF_8);
        try (final KafkaProducer<Void, byte[]> producer = producer(new VoidSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        final byte[][] data = new byte[][] {value};
        valueTest(Type.byteType().arrayType(), new ByteArrayDeserializer(), ObjectChunk.chunkWrap(data));
    }

    @Test
    void shortValue() throws ExecutionException, InterruptedException {
        final Short value = 42;
        try (final KafkaProducer<Void, Short> producer = producer(new VoidSerializer(), new ShortSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.shortType().boxedType(), new ShortDeserializer(), ShortChunk.chunkWrap(new short[] {value}));
    }

    @Test
    void intValue() throws ExecutionException, InterruptedException {
        final Integer value = 42;
        try (final KafkaProducer<Void, Integer> producer = producer(new VoidSerializer(), new IntegerSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.intType().boxedType(), new IntegerDeserializer(), IntChunk.chunkWrap(new int[] {value}));
    }

    @Test
    void longValue() throws ExecutionException, InterruptedException {
        final Long value = 42L;
        try (final KafkaProducer<Void, Long> producer = producer(new VoidSerializer(), new LongSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.longType().boxedType(), new LongDeserializer(), LongChunk.chunkWrap(new long[] {value}));
    }

    @Test
    void floatValue() throws ExecutionException, InterruptedException {
        final Float value = 42.0f;
        try (final KafkaProducer<Void, Float> producer = producer(new VoidSerializer(), new FloatSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.floatType().boxedType(), new FloatDeserializer(), FloatChunk.chunkWrap(new float[] {value}));
    }

    @Test
    void doubleValue() throws ExecutionException, InterruptedException {
        final Double value = 42.0d;
        try (final KafkaProducer<Void, Double> producer = producer(new VoidSerializer(), new DoubleSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.doubleType().boxedType(), new DoubleDeserializer(), DoubleChunk.chunkWrap(new double[] {value}));
    }

    @Test
    void customRecordProcessor() throws ExecutionException, InterruptedException {
        final RecordMetadata metadata;
        try (final KafkaProducer<String, String> producer = producer(new StringSerializer(), new StringSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, "myKey", "myValue")).get();
        }
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<String, String> publishersImpl = PublishersOptions.<String, String>builder()
                .clientOptions(clientOptions(new StringDeserializer(), new StringDeserializer()))
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.beginning(topic))
                .processor(ObjectProcessor.combined(List.of(
                        Processors.key(CharSequenceLength.INSTANCE),
                        Processors.value(CharSequenceLength.INSTANCE))))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(1);
        }
        recorder.flushPublisher();
        recorder.assertEquals(IntChunk.chunkWrap(new int[] {5}), IntChunk.chunkWrap(new int[] {7}));
        recorder.close();
    }

    @Test
    void receiveTimestamp() throws InterruptedException, ExecutionException {
        final RecordMetadata metadata;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.beginning(topic))
                .processor(ObjectProcessor.empty())
                .chunkSize(1024)
                .receiveTimestamp(true)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(1);
        }
        recorder.flushPublisher();
        final Chunk<?> chunk = recorder.singleValue();
        assertThat(chunk.size()).isEqualTo(1);
        assertThat(chunk.getChunkType()).isEqualTo(ChunkType.Long);
        assertThat(chunk.asLongChunk().get(0)).isPositive();
        recorder.close();
    }

    private static final ToLongFunction<ConsumerRecord<?, ?>> OFFSET_FUNCTION = ConsumerRecord::offset;

    private static final ToIntFunction<ConsumerRecord<?, ?>> PARTITION_FUNCTION = ConsumerRecord::partition;

    private static final ToObjectFunction<ConsumerRecord<?, ?>, String> TOPIC_FUNCTION =
            ToObjectFunction.of(ConsumerRecord::topic, Type.stringType());

    @Test
    void offsetsEnd() throws ExecutionException, InterruptedException {
        final RecordMetadata metadata_1;
        final RecordMetadata metadata_2;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_1 = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.end(topic))
                .processor(ObjectProcessorFunctions.of(List.of(OFFSET_FUNCTION)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            // We need to ensure our send happens _after_ we've established our Offsets.end(topic) position, so we wait
            // for 0 records, which indicates that at least one poll has passed.
            publishersImpl.awaitRecords(0);
            try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
                metadata_2 = producer.send(new ProducerRecord<>(topic, null, null)).get();
            }
            publishersImpl.awaitRecords(1);
        }
        recorder.flushPublisher();
        recorder.assertEquals(LongChunk.chunkWrap(new long[] {metadata_2.offset()}));
        recorder.close();
    }

    @Test
    void offsetsCommitted() throws ExecutionException, InterruptedException {
        final RecordMetadata metadata_1;
        final RecordMetadata metadata_2;
        try (
                final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer());
                final KafkaConsumer<Void, Void> consumer = consumer(new VoidDeserializer(), new VoidDeserializer(),
                        Map.of(
                                ConsumerConfig.GROUP_ID_CONFIG, "test-group-id",
                                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"))) {
            metadata_1 = producer.send(new ProducerRecord<>(topic, null, null)).get();
            final TopicPartition tp = new TopicPartition(metadata_1.topic(), metadata_1.partition());
            consumer.assign(Set.of(tp));
            consumer.seek(tp, metadata_1.offset() + 1);
            consumer.poll(Duration.ZERO);
            consumer.commitSync();
            metadata_2 = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(ClientOptions.<Void, Void>builder()
                        .keyDeserializer(new VoidDeserializer())
                        .valueDeserializer(new VoidDeserializer())
                        .putAllConfig(consumerConfig())
                        .putConfig(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id")
                        .putConfig(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                        .build())
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.committed(topic))
                .processor(ObjectProcessorFunctions.of(List.of(OFFSET_FUNCTION)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(1);
        }
        recorder.flushPublisher();
        recorder.assertEquals(LongChunk.chunkWrap(new long[] {metadata_2.offset()}));
        recorder.close();
    }

    @Test
    void offsetsExplicit() throws ExecutionException, InterruptedException {
        final RecordMetadata metadata_1;
        final RecordMetadata metadata_2;
        final RecordMetadata metadata_3;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_1 = producer.send(new ProducerRecord<>(topic, null, null)).get();
            metadata_2 = producer.send(new ProducerRecord<>(topic, null, null)).get();
            metadata_3 = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.of(new TopicPartition(topic, 0), metadata_2.offset()))
                .processor(ObjectProcessorFunctions.of(List.of(OFFSET_FUNCTION)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(2);
        }
        recorder.flushPublisher();
        recorder.assertEquals(LongChunk.chunkWrap(new long[] {metadata_2.offset(), metadata_3.offset()}));
        recorder.close();
    }

    @Test
    void offsetsTimestamp() throws ExecutionException, InterruptedException {
        final long epochMillis = 1706238105939L;
        final RecordMetadata metadata_1;
        final RecordMetadata metadata_2;
        final RecordMetadata metadata_3;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_1 = producer.send(new ProducerRecord<>(topic, null, epochMillis - 1, null, null, null)).get();
            metadata_2 = producer.send(new ProducerRecord<>(topic, null, epochMillis, null, null, null)).get();
            metadata_3 = producer.send(new ProducerRecord<>(topic, null, epochMillis + 1, null, null, null)).get();
        }
        {
            final StreamConsumerRecorder recorder;
            try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                    .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                    .partitioning(Partitioning.single())
                    .filter(x -> true)
                    .offsets(Offsets.timestamp(topic, Instant.ofEpochMilli(epochMillis)))
                    .processor(ObjectProcessorFunctions.of(List.of(OFFSET_FUNCTION)))
                    .chunkSize(1024)
                    .receiveTimestamp(false)
                    .build()
                    .publishersImpl()) {
                try {
                    recorder = singleRecorder(publishersImpl.publishers());
                    publishersImpl.start();
                } catch (Throwable t) {
                    publishersImpl.errorBeforeStart(t);
                    throw t;
                }
                publishersImpl.awaitRecords(2);
            }
            recorder.flushPublisher();
            recorder.assertEquals(LongChunk.chunkWrap(new long[] {metadata_2.offset(), metadata_3.offset()}));
            recorder.close();
        }
        {
            final StreamConsumerRecorder recorder;
            try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                    .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                    .partitioning(Partitioning.single())
                    .filter(x -> true)
                    .offsets(Offsets.timestamp(topic, Instant.ofEpochMilli(epochMillis).plusNanos(1)))
                    .processor(ObjectProcessorFunctions.of(List.of(OFFSET_FUNCTION)))
                    .chunkSize(1024)
                    .receiveTimestamp(false)
                    .build()
                    .publishersImpl()) {
                try {
                    recorder = singleRecorder(publishersImpl.publishers());
                    publishersImpl.start();
                } catch (Throwable t) {
                    publishersImpl.errorBeforeStart(t);
                    throw t;
                }
                publishersImpl.awaitRecords(1);
            }
            recorder.flushPublisher();
            recorder.assertEquals(LongChunk.chunkWrap(new long[] {metadata_3.offset()}));
            recorder.close();
        }
    }

    @Test
    void filter() throws ExecutionException, InterruptedException {
        final RecordMetadata metadata_1;
        final RecordMetadata metadata_2;
        final RecordMetadata metadata_3;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_1 = producer.send(new ProducerRecord<>(topic, null, null, null, null,
                    List.of(new RecordHeader("DoNotSkip", new byte[0])))).get();
            metadata_2 = producer.send(new ProducerRecord<>(topic, null, null)).get();
            metadata_3 = producer.send(new ProducerRecord<>(topic, null, null, null, null,
                    List.of(new RecordHeader("DoNotSkip", new byte[0])))).get();
        }
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.single())
                .filter(record -> record.headers().lastHeader("DoNotSkip") != null)
                .offsets(Offsets.beginning(topic))
                .processor(ObjectProcessorFunctions.of(List.of(OFFSET_FUNCTION)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(3);
        }
        recorder.flushPublisher();
        recorder.assertEquals(LongChunk.chunkWrap(new long[] {metadata_1.offset(), metadata_3.offset()}));
        recorder.close();
    }

    @Test
    void chunkSize() throws ExecutionException, InterruptedException {
        final RecordMetadata metadata_1;
        final RecordMetadata metadata_2;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_1 = producer.send(new ProducerRecord<>(topic, null, null)).get();
            metadata_2 = producer.send(new ProducerRecord<>(topic, null, null)).get();
        }
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.beginning(topic))
                .processor(ObjectProcessorFunctions.of(List.of(OFFSET_FUNCTION)))
                .chunkSize(1)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(2);
        }
        recorder.flushPublisher();
        recorder.assertNextChunksEquals(LongChunk.chunkWrap(new long[] {metadata_1.offset()}));
        recorder.assertNextChunksEquals(LongChunk.chunkWrap(new long[] {metadata_2.offset()}));
        recorder.assertEmpty();
        recorder.close();
    }

    @Test
    void multipleTopics() throws ExecutionException, InterruptedException {
        final String topic2 = topic + "-again";
        createTopicImpl(topic2, 1);
        final RecordMetadata metadata_t1_1;
        final RecordMetadata metadata_t2_1;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_t1_1 = producer.send(new ProducerRecord<>(topic, null, null)).get();
            metadata_t2_1 = producer.send(new ProducerRecord<>(topic2, null, null)).get();
        }
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.of(Offsets.beginning(topic), Offsets.beginning(topic2)))
                .processor(ObjectProcessorFunctions.of(List.of(TOPIC_FUNCTION, OFFSET_FUNCTION)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(2);
        }
        recorder.flushPublisher();
        // If we were doing this w/ table ops, we could sort the results; this is an easy enough two-case scenario.
        try {
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {topic, topic2}),
                    LongChunk.chunkWrap(new long[] {metadata_t1_1.offset(), metadata_t2_1.offset()}));
        } catch (AssertionFailedError e) {
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {topic2, topic}),
                    LongChunk.chunkWrap(new long[] {metadata_t2_1.offset(), metadata_t1_1.offset()}));
        }
        recorder.close();
    }

    @Test
    void perTopicPartitionPartitioning() throws ExecutionException, InterruptedException {
        final String topic1 = topic + "-topic1";
        final String topic2 = topic + "-topic2";
        createTopicImpl(topic1, 2);
        createTopicImpl(topic2, 2);
        final RecordMetadata metadata_t1_p0;
        final RecordMetadata metadata_t1_p1;
        final RecordMetadata metadata_t2_p0;
        final RecordMetadata metadata_t2_p1;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_t1_p0 = producer.send(new ProducerRecord<>(topic1, 0, null, null)).get();
            metadata_t1_p1 = producer.send(new ProducerRecord<>(topic1, 1, null, null)).get();
            metadata_t2_p0 = producer.send(new ProducerRecord<>(topic2, 0, null, null)).get();
            metadata_t2_p1 = producer.send(new ProducerRecord<>(topic2, 1, null, null)).get();
        }
        final Map<TopicPartition, StreamConsumerRecorder> recorders;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.perTopicPartition())
                .filter(x -> true)
                .offsets(Offsets.of(Offsets.beginning(topic1), Offsets.beginning(topic2)))
                .processor(ObjectProcessorFunctions.of(List.of(TOPIC_FUNCTION, PARTITION_FUNCTION)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {

            try {
                assertThat(publishersImpl.publishers()).hasSize(4);
                recorders = new HashMap<>(4);
                for (Publisher publisher : publishersImpl.publishers()) {
                    final TopicPartition tp = single(publisher.topicPartitions());
                    final StreamConsumerRecorder consumer = new StreamConsumerRecorder(publisher);
                    publisher.register(consumer);
                    recorders.put(tp, consumer);
                }
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(4);
        }
        for (Entry<TopicPartition, StreamConsumerRecorder> e : recorders.entrySet()) {
            final TopicPartition tp = e.getKey();
            final StreamConsumerRecorder recorder = e.getValue();
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {tp.topic()}),
                    IntChunk.chunkWrap(new int[] {tp.partition()}));
        }
        for (StreamConsumerRecorder recorder : recorders.values()) {
            recorder.close();
        }
    }

    @Test
    void perTopicPartitioning() throws ExecutionException, InterruptedException {
        final String topic1 = topic + "-topic1";
        final String topic2 = topic + "-topic2";
        createTopicImpl(topic1, 2);
        createTopicImpl(topic2, 2);
        final RecordMetadata metadata_t1_p0;
        final RecordMetadata metadata_t1_p1;
        final RecordMetadata metadata_t2_p0;
        final RecordMetadata metadata_t2_p1;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_t1_p0 = producer.send(new ProducerRecord<>(topic1, 0, null, null)).get();
            metadata_t1_p1 = producer.send(new ProducerRecord<>(topic1, 1, null, null)).get();
            metadata_t2_p0 = producer.send(new ProducerRecord<>(topic2, 0, null, null)).get();
            metadata_t2_p1 = producer.send(new ProducerRecord<>(topic2, 1, null, null)).get();
        }
        final Map<String, StreamConsumerRecorder> recorders;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.perTopic())
                .filter(x -> true)
                .offsets(Offsets.of(Offsets.beginning(topic1), Offsets.beginning(topic2)))
                .processor(ObjectProcessorFunctions.of(List.of(TOPIC_FUNCTION, PARTITION_FUNCTION)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {

            try {
                assertThat(publishersImpl.publishers()).hasSize(2);
                recorders = new HashMap<>(2);
                for (Publisher publisher : publishersImpl.publishers()) {
                    final String topic = single(publisher.topicPartitions().stream().map(TopicPartition::topic)
                            .collect(Collectors.toSet()));
                    final StreamConsumerRecorder consumer = new StreamConsumerRecorder(publisher);
                    publisher.register(consumer);
                    recorders.put(topic, consumer);
                }
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(4);
        }
        for (Entry<String, StreamConsumerRecorder> e : recorders.entrySet()) {
            final String topic = e.getKey();
            final StreamConsumerRecorder recorder = e.getValue();
            recorder.flushPublisher();
            try {
                recorder.assertEquals(
                        ObjectChunk.chunkWrap(new String[] {topic, topic}),
                        IntChunk.chunkWrap(new int[] {0, 1}));
            } catch (AssertionFailedError e2) {
                recorder.assertEquals(
                        ObjectChunk.chunkWrap(new String[] {topic, topic}),
                        IntChunk.chunkWrap(new int[] {1, 0}));
            }
        }
        for (StreamConsumerRecorder recorder : recorders.values()) {
            recorder.close();
        }
    }

    @Test
    void perPartitionPartitioning() throws ExecutionException, InterruptedException {
        final String topic1 = topic + "-topic1";
        final String topic2 = topic + "-topic2";
        createTopicImpl(topic1, 2);
        createTopicImpl(topic2, 2);
        final RecordMetadata metadata_t1_p0;
        final RecordMetadata metadata_t1_p1;
        final RecordMetadata metadata_t2_p0;
        final RecordMetadata metadata_t2_p1;
        try (final KafkaProducer<Void, Void> producer = producer(new VoidSerializer(), new VoidSerializer())) {
            metadata_t1_p0 = producer.send(new ProducerRecord<>(topic1, 0, null, null)).get();
            metadata_t1_p1 = producer.send(new ProducerRecord<>(topic1, 1, null, null)).get();
            metadata_t2_p0 = producer.send(new ProducerRecord<>(topic2, 0, null, null)).get();
            metadata_t2_p1 = producer.send(new ProducerRecord<>(topic2, 1, null, null)).get();
        }
        final Map<Integer, StreamConsumerRecorder> recorders;
        try (final PublishersImpl<Void, Void> publishersImpl = PublishersOptions.<Void, Void>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), new VoidDeserializer()))
                .partitioning(Partitioning.perPartition())
                .filter(x -> true)
                .offsets(Offsets.of(Offsets.beginning(topic1), Offsets.beginning(topic2)))
                .processor(ObjectProcessorFunctions.of(List.of(TOPIC_FUNCTION, PARTITION_FUNCTION)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {

            try {
                assertThat(publishersImpl.publishers()).hasSize(2);
                recorders = new HashMap<>(2);
                for (Publisher publisher : publishersImpl.publishers()) {
                    final Integer partition = single(publisher.topicPartitions().stream().map(TopicPartition::partition)
                            .collect(Collectors.toSet()));
                    final StreamConsumerRecorder consumer = new StreamConsumerRecorder(publisher);
                    publisher.register(consumer);
                    recorders.put(partition, consumer);
                }
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(4);
        }
        for (Entry<Integer, StreamConsumerRecorder> e : recorders.entrySet()) {
            final int partition = e.getKey();
            final StreamConsumerRecorder recorder = e.getValue();
            recorder.flushPublisher();
            try {
                recorder.assertEquals(
                        ObjectChunk.chunkWrap(new String[] {topic1, topic2}),
                        IntChunk.chunkWrap(new int[] {partition, partition}));
            } catch (AssertionFailedError e2) {
                recorder.assertEquals(
                        ObjectChunk.chunkWrap(new String[] {topic2, topic1}),
                        IntChunk.chunkWrap(new int[] {partition, partition}));
            }
        }
        for (StreamConsumerRecorder recorder : recorders.values()) {
            recorder.close();
        }
    }

    private <K> void keyTest(GenericType<K> keyType, Deserializer<K> keyDeserializer, Chunk<?> expected)
            throws InterruptedException {
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<K, Void> publishersImpl = PublishersOptions.<K, Void>builder()
                .clientOptions(clientOptions(keyDeserializer, new VoidDeserializer()))
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.beginning(topic))
                .processor(Processors.key(ObjectProcessorFunctions.identity(keyType)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(expected.size());
        }
        recorder.flushPublisher();
        recorder.assertEquals(expected);
        recorder.close();
    }

    private <V> void valueTest(GenericType<V> valueType, Deserializer<V> valueDeserializer, Chunk<?> expected)
            throws InterruptedException {
        final StreamConsumerRecorder recorder;
        try (final PublishersImpl<Void, V> publishersImpl = PublishersOptions.<Void, V>builder()
                .clientOptions(clientOptions(new VoidDeserializer(), valueDeserializer))
                .partitioning(Partitioning.single())
                .filter(x -> true)
                .offsets(Offsets.beginning(topic))
                .processor(Processors.value(ObjectProcessorFunctions.identity(valueType)))
                .chunkSize(1024)
                .receiveTimestamp(false)
                .build()
                .publishersImpl()) {
            try {
                recorder = singleRecorder(publishersImpl.publishers());
                publishersImpl.start();
            } catch (Throwable t) {
                publishersImpl.errorBeforeStart(t);
                throw t;
            }
            publishersImpl.awaitRecords(expected.size());
        }
        recorder.flushPublisher();
        recorder.assertEquals(expected);
        recorder.close();
    }

    private static StreamConsumerRecorder singleRecorder(Collection<? extends Publisher> publishers) {
        final Publisher publisher = single(publishers);
        final StreamConsumerRecorder consumer = new StreamConsumerRecorder(publisher);
        publisher.register(consumer);
        return consumer;
    }

    final Admin admin() {
        return Admin.create(adminConfig());
    }

    final <K, V> KafkaProducer<K, V> producer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new KafkaProducer<>(producerConfig(), keySerializer, valueSerializer);
    }

    final <K, V> KafkaConsumer<K, V> consumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        return new KafkaConsumer<K, V>((Map) consumerConfig(), keyDeserializer, valueDeserializer);
    }

    final <K, V> KafkaConsumer<K, V> consumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
            Map<String, String> extras) {
        final Map<String, String> config = new HashMap<>(consumerConfig());
        config.putAll(extras);
        return new KafkaConsumer<K, V>((Map) config, keyDeserializer, valueDeserializer);
    }

    private <K, V> ClientOptions<K, V> clientOptions(Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        return ClientOptions.<K, V>builder()
                .putAllConfig(consumerConfig())
                .keyDeserializer(keyDeserializer)
                .valueDeserializer(valueDeserializer)
                .build();
    }


    private static <T> T single(Iterable<T> iterable) {
        final Iterator<T> it = iterable.iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException();
        }
        final T single = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException();
        }
        return single;
    }

    private static class StreamConsumerRecorder implements StreamConsumer, Closeable {
        final Publisher publisher;
        final List<WritableChunk<Values>[]> accepted = new ArrayList<>();
        final List<Throwable> failures = new ArrayList<>();
        // private final LongAdder rowCount = new LongAdder();

        private StreamConsumerRecorder(Publisher publisher) {
            this.publisher = Objects.requireNonNull(publisher);
        }

        public void flushPublisher() {
            publisher.flush();
        }

        public synchronized void clear() {
            if (!failures.isEmpty()) {
                throw new IllegalStateException();
            }
            accepted.clear();
        }

        @Override
        public synchronized void accept(@NotNull WritableChunk<Values>... data) {
            // rowCount.add(data[0].size());
            accepted.add(data);
        }

        @Override
        public synchronized void accept(@NotNull Collection<WritableChunk<Values>[]> data) {
            // rowCount.add(data.stream().map(x -> x[0]).mapToInt(Chunk::size).sum());
            accepted.addAll(data);
        }

        @Override
        public synchronized void acceptFailure(@NotNull Throwable cause) {
            failures.add(cause);
        }

        @Override
        public void close() {
            publisher.shutdown();
        }

        public synchronized Chunk<?> singleValue() {
            assertThat(failures).isEmpty();
            assertThat(accepted).hasSize(1);
            assertThat(accepted.get(0)).hasSize(1);
            return accepted.get(0)[0];
        }

        public synchronized void assertEquals(Chunk<?>... expectedChunks) {
            assertThat(failures).isEmpty();
            assertThat(accepted).hasSize(1);
            assertNextChunksEquals(expectedChunks);
        }

        public synchronized void assertEmpty() {
            assertThat(failures).isEmpty();
            assertThat(accepted).isEmpty();
        }

        public synchronized void assertNextChunksEquals(Chunk<?>... expectedChunks) {
            assertThat(failures).isEmpty();
            final Iterator<WritableChunk<Values>[]> it = accepted.iterator();
            assertThat(it).hasNext();
            final WritableChunk<Values>[] chunks = it.next();
            assertThat(chunks).hasSize(expectedChunks.length);
            for (int i = 0; i < chunks.length; ++i) {
                assertThat(chunks[i]).usingComparator(FAKE_COMPARE_FOR_EQUALS).isEqualTo(expectedChunks[i]);
            }
            it.remove();
        }
    }

    private void createTopicImpl(String name, int numPartitions) throws InterruptedException, ExecutionException {
        try (final Admin admin = admin()) {
            admin.createTopics(List.of(new NewTopic(name, Optional.of(numPartitions), Optional.empty()))).all().get();
        }
    }

    private static final Comparator<Chunk<?>> FAKE_COMPARE_FOR_EQUALS =
            PublishersOptionsBase::fakeCompareForEquals;

    private static int fakeCompareForEquals(Chunk<?> x, Chunk<?> y) {
        return ChunkEquals.equals(x, y) ? 0 : 1;
    }

    enum CharSequenceLength implements ObjectProcessor<CharSequence> {
        INSTANCE;

        @Override
        public int size() {
            return 1;
        }

        @Override
        public List<Type<?>> outputTypes() {
            return List.of(Type.intType());
        }

        @Override
        public void processAll(ObjectChunk<? extends CharSequence, ?> in, List<WritableChunk<?>> out) {
            final WritableIntChunk<?> lengthOut = out.get(0).asWritableIntChunk();
            for (int i = 0; i < in.size(); ++i) {
                lengthOut.add(in.get(i).length());
            }
        }
    }
}
