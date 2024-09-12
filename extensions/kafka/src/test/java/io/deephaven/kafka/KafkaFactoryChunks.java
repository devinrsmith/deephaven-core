//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.processor.factory.EventProcessorFactories;
import io.deephaven.processor.factory.EventProcessorFactory;
import io.deephaven.processor.factory.EventProcessorSpec;
import io.deephaven.processor.factory.EventProcessorStreamSpec;
import io.deephaven.processor.factory.EventProcessors;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.QueryConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

final class KafkaFactoryChunks {

    public static EventProcessorFactory<ConsumerRecord<?, ?>> basics() {
        // todo: give the ability to append onto a singleStream(), must specify it is exactly 1 output
        return BasicsFactory.BASICS;
    }

    public static EventProcessorFactory<ConsumerRecord<?, ?>> headers() {
        return HeadersFactory.HEADERS;
    }

    public static EventProcessorFactory<ConsumerRecord<?, ?>> all() {
        return EventProcessorFactories.concat(basics(), headers(), false);
    }


    private enum BasicsFactory implements EventProcessorFactory<ConsumerRecord<?, ?>> {
        BASICS;

        @Override
        public EventProcessorSpec spec() {
            return EventProcessorSpec.builder()
                    .usesCoordinator(false)
                    .addStreams(EventProcessorStreamSpec.builder()
                            .isRowOriented(true)
                            .expectedSize(1)
                            .addAllOutputTypes(Basics.TYPES)
                            .build())
                    .build();
        }

        @Override
        public EventProcessor<ConsumerRecord<?, ?>> create(Sink sink) {
            final Basics basics = new Basics(sink.singleStream());
            return EventProcessors.noClose(basics::write);
        }
    }

    private enum HeadersFactory implements EventProcessorFactory<ConsumerRecord<?, ?>> {
        HEADERS;

        @Override
        public EventProcessorSpec spec() {
            return EventProcessorSpec.builder()
                    .usesCoordinator(false)
                    .addStreams(EventProcessorStreamSpec.builder()
                            .isRowOriented(true)
                            .addAllOutputTypes(Headers.TYPES)
                            .build())
                    .build();
        }

        @Override
        public EventProcessor<ConsumerRecord<?, ?>> create(Sink sink) {
            final Headers headers = new Headers(sink.singleStream());
            return EventProcessors.noClose(headers::write);
        }
    }

    private static final class Key {
        private static final List<Type<?>> TYPES = List.of(
                Type.stringType(),
                Type.intType(),
                Type.longType());

        private final WritableObjectChunk<String, ?> topic;
        private final WritableIntChunk<?> partition;
        private final WritableLongChunk<?> offset;

        Key(int chunkSize) {
            topic = WritableObjectChunk.makeWritableChunk(chunkSize);
            partition = WritableIntChunk.makeWritableChunk(chunkSize);
            offset = WritableLongChunk.makeWritableChunk(chunkSize);
        }

        public void set(int pos, ConsumerRecord<?, ?> record) {
            topic.set(pos, record.topic());
            partition.set(pos, record.partition());
            offset.set(pos, record.offset());
        }
    }

    private static final class Basics implements StreamPublisher {
        private static final List<Type<?>> TYPES = java.util.stream.Stream.concat(Key.TYPES.stream(),
                java.util.stream.Stream.of(
                        Type.instantType(),
                        Type.ofCustom(TimestampType.class),
                        Type.intType(),
                        Type.intType(),
                        Type.intType()))
                .collect(Collectors.toUnmodifiableList());

        private static final int CHUNK_SIZE = 1024;

        private final Stream stream;

        private int pos;
        private Key key;
        private WritableLongChunk<?> timestamp;
        private WritableObjectChunk<TimestampType, ?> timestampType;
        private WritableIntChunk<?> serializedKeySize;
        private WritableIntChunk<?> serializedValueSize;
        private WritableIntChunk<?> leaderEpoch;
        // todo: raw key, raw value?

        private StreamConsumer consumer;

        public Basics(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            init();
        }

        private void init() {
            pos = 0;
            key = new Key(CHUNK_SIZE);
            timestamp = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
            timestampType = WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
            leaderEpoch = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            serializedKeySize = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            serializedValueSize = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
        }

        public void write(ConsumerRecord<?, ?> record) {
            key.set(pos, record);
            timestamp.set(pos, record.timestamp() * 1_000_000);
            timestampType.set(pos, record.timestampType());
            leaderEpoch.set(pos, record.leaderEpoch().orElse(QueryConstants.NULL_INT));
            serializedKeySize.set(pos, record.serializedKeySize());
            serializedValueSize.set(pos, record.serializedValueSize());
            ++pos;
            if (pos == CHUNK_SIZE) {
                flushImpl();
            }
        }

        public void flushImpl() {
            if (pos == 0) {
                return;
            }
            try {
                // noinspection unchecked
                consumer.accept(new WritableChunk[] {key.topic, key.partition, key.offset, timestamp, timestampType,
                        leaderEpoch, serializedKeySize, serializedValueSize});
            } finally {
                init();
            }
        }

        //

        @Override
        public void register(@NotNull StreamConsumer consumer) {

        }

        @Override
        public void flush() {
            flushImpl();
        }

        @Override
        public void shutdown() {

        }
    }

    private static final class Headers {
        private static final List<Type<?>> TYPES = java.util.stream.Stream.concat(Key.TYPES.stream(),
                java.util.stream.Stream.of(
                        Type.stringType(),
                        Type.byteType().arrayType()))
                .collect(Collectors.toUnmodifiableList());

        private final Stream stream;
        private final Key key;
        private final ObjectAppender<String> headerKey;
        private final ObjectAppender<byte[]> headerValue;

        public Headers(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            key = null;// new Key(stream);
            headerKey = ObjectAppender.get(stream.appenders().get(3), Type.stringType());
            headerValue = ObjectAppender.get(stream.appenders().get(4), Type.byteType().arrayType());
        }

        public void write(ConsumerRecord<?, ?> record) {
            for (Header header : record.headers()) {
                // key.set(record);
                headerKey.set(header.key());
                headerValue.set(header.value());
                stream.advanceAll();
            }
        }
    }

}
