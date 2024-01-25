/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkEquals;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.testutil.junit5.EngineExtensions;
import io.deephaven.kafka.v2.PublishersOptions.Partitioning;
import io.deephaven.kafka.v2.TopicExtension.Topic;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
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
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith({TopicExtension.class, EngineExtensions.class})
public abstract class PublishersOptionsSingleTopicBase {

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
    void intKey() throws ExecutionException, InterruptedException {
        final Integer key = 42;
        try (final KafkaProducer<Integer, Void> producer = producer(new IntegerSerializer(), new VoidSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, null)).get();
        }
        keyTest(Type.intType().boxedType(), new IntegerDeserializer(), IntChunk.chunkWrap(new int[] {key}));
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
    void intValue() throws ExecutionException, InterruptedException {
        final Integer value = 42;
        try (final KafkaProducer<Void, Integer> producer = producer(new VoidSerializer(), new IntegerSerializer())) {
            producer.send(new ProducerRecord<>(topic, null, value)).get();
        }
        valueTest(Type.intType().boxedType(), new IntegerDeserializer(), IntChunk.chunkWrap(new int[] {value}));
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

        @Override
        public void accept(@NotNull WritableChunk<Values>... data) {
            // rowCount.add(data[0].size());
            accepted.add(data);
        }

        @Override
        public void accept(@NotNull Collection<WritableChunk<Values>[]> data) {
            // rowCount.add(data.stream().map(x -> x[0]).mapToInt(Chunk::size).sum());
            accepted.addAll(data);
        }

        @Override
        public void acceptFailure(@NotNull Throwable cause) {
            failures.add(cause);
        }

        @Override
        public void close() {
            publisher.shutdown();
        }

        public Chunk<?> singleValue() {
            assertThat(accepted).hasSize(1);
            assertThat(accepted.get(0)).hasSize(1);
            return accepted.get(0)[0];
        }

        public void assertEquals(Chunk<?>... expectedChunks) {
            assertThat(accepted).hasSize(1);
            final WritableChunk<Values>[] chunks = accepted.get(0);
            assertThat(chunks).hasSize(expectedChunks.length);
            for (int i = 0; i < chunks.length; ++i) {
                assertThat(chunks[i]).usingComparator(FAKE_COMPARE_FOR_EQUALS).isEqualTo(expectedChunks[i]);
            }
        }
    }

    private static final Comparator<Chunk<?>> FAKE_COMPARE_FOR_EQUALS =
            PublishersOptionsSingleTopicBase::fakeCompareForEquals;

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
