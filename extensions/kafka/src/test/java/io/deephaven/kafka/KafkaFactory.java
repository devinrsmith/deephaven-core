//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.processor.factory.EventProcessorFactories;
import io.deephaven.processor.factory.EventProcessorFactory;
import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.factory.EventProcessorSpec;
import io.deephaven.processor.factory.EventProcessorStreamSpec;
import io.deephaven.processor.factory.EventProcessorStreamSpec.Key;
import io.deephaven.processor.factory.EventProcessors;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

final class KafkaFactory {

    // -------------

    public static final Key<String> TOPIC = new Key<>("Topic", Type.stringType());

    public static final Key<Integer> PARTITION = new Key<>("Partition", Type.intType());

    public static final Key<Long> OFFSET = new Key<>("Offset", Type.longType());

    // -------------

    public static final Key<Instant> TIMESTAMP = new Key<>("Timestamp", Type.instantType());

    public static final Key<TimestampType> TIMESTAMP_TYPE = new Key<>("TimestampType", Type.ofCustom(TimestampType.class));

    public static final Key<Integer> SERIALIZED_KEY_SIZE = new Key<>("SerializedKeySize", Type.intType());

    public static final Key<Integer> SERIALIZED_VALUE_SIZE = new Key<>("SerializedValueSize", Type.intType());

    public static final Key<Integer> LEADER_EPOCH = new Key<>("LeaderEpoch", Type.intType());

    // -------------

    public static final Key<String> HEADER_KEY = new Key<>("HeaderKey", Type.stringType());

    public static final Key<byte[]> HEADER_VALUE = new Key<>("HeaderValue", Type.byteType().arrayType());

    // -------------


    public static EventProcessorStreamSpec basics(Key<?>... keys) {
        return EventProcessorStreamSpec.builder()
                .addKeys(keys)
                .isRowOriented(true)
                .expectedSize(1)
                .build();
    }

    public static EventProcessorSpec spec(Key<?>... keys) {
        return null;
    }

    public static EventProcessorFactory<ConsumerRecord<?, ?>> factory(Key<?>... keys) {
        return EventProcessorFactories.of(spec(keys), new Function<Sink, EventProcessor<ConsumerRecord<?, ?>>>() {
            @Override
            public EventProcessor<ConsumerRecord<?, ?>> apply(Sink sink) {
                return null;
            }
        });
    }


    public static EventProcessorFactory<ConsumerRecord<?, ?>> basics() {
        // todo: give the ability to append onto a singleStream(), must specify it is exactly 1 output
        return BasicsFactory.BASICS;
    }

    public static EventProcessorFactory<ConsumerRecord<?, ?>> headers() {
        return HeadersFactory.HEADERS;
    }

    public static <K> EventProcessorFactory<ConsumerRecord<K, ?>> key(GenericType<K> keyType) {
        final EventProcessorSpec spec = EventProcessorSpec.builder()
                .usesCoordinator(false)
                .addStreams(singletonSpec(keyType))
                .build();
        return EventProcessorFactories.map(ConsumerRecord::key, EventProcessorFactories.singleton(keyType, spec));
    }

    public static <V> EventProcessorFactory<ConsumerRecord<?, V>> value(GenericType<V> valueType) {
        final EventProcessorSpec spec = EventProcessorSpec.builder()
                .usesCoordinator(false)
                .addStreams(singletonSpec(valueType))
                .build();
        return EventProcessorFactories.map(ConsumerRecord::value, EventProcessorFactories.singleton(valueType, spec));
    }

    private static <T> EventProcessorStreamSpec singletonSpec(GenericType<T> keyType) {
        return EventProcessorStreamSpec.builder()
                .isRowOriented(false)
                .expectedSize(1)
                .key(keyType)
                .build();
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
            return EventProcessors.noClose(basics::set);
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

    private static final class Keyk {
        private static final List<Type<?>> TYPES = List.of(
                Type.stringType(),
                Type.intType(),
                Type.longType());

        public static Keyk from(Stream stream) {
            return new Keyk(
                    ObjectAppender.getIfPresent(stream, TOPIC),
                    IntAppender.getIfPresent(stream, PARTITION),
                    LongAppender.get(stream, OFFSET));
        }

        private final ObjectAppender<String> topic;
        private final IntAppender partition;
        private final LongAppender offset;

        private Keyk(ObjectAppender<String> topic, IntAppender partition, LongAppender offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = Objects.requireNonNull(offset);
        }

        public void set(ConsumerRecord<?, ?> record) {
            if (topic != null) {
                topic.set(record.topic());
            }
            if (partition != null) {
                partition.set(record.partition());
            }
            offset.set(record.offset());
        }
    }

    private static final class Basics {
        private static final List<Type<?>> TYPES = java.util.stream.Stream.concat(Keyk.TYPES.stream(),
                java.util.stream.Stream.of(
                        Type.instantType(),
                        Type.ofCustom(TimestampType.class),
                        Type.intType(),
                        Type.intType(),
                        Type.intType()))
                .collect(Collectors.toUnmodifiableList());


        public static Basics from(Stream stream) {
            return new Basics(
                    Keyk.from(stream),
                    InstantAppender.getIfPresent(stream, TIMESTAMP),
                    ObjectAppender.getIfPresent(stream, TIMESTAMP_TYPE),
                    IntAppender.getIfPresent(stream, SERIALIZED_KEY_SIZE),
                    IntAppender.getIfPresent(stream, SERIALIZED_VALUE_SIZE),
                    IntAppender.getIfPresent(stream, LEADER_EPOCH));
        }

        private final Keyk key;
        private final LongAppender timestampMillis;
        private final ObjectAppender<TimestampType> timestampType;
        private final IntAppender serializedKeySize;
        private final IntAppender serializedValueSize;
        private final IntAppender leaderEpoch;

        private Basics(Keyk key, InstantAppender timestamp, ObjectAppender<TimestampType> timestampType, IntAppender serializedKeySize, IntAppender serializedValueSize, IntAppender leaderEpoch) {
            this.key = key;
            this.timestampMillis = timestamp == null ? null : timestamp.asLongEpochAppender(TimeUnit.MILLISECONDS);
            this.timestampType = timestampType;
            this.serializedKeySize = serializedKeySize;
            this.serializedValueSize = serializedValueSize;
            this.leaderEpoch = leaderEpoch;
        }

        public void set(ConsumerRecord<?, ?> record) {
            key.set(record);
            if (timestampMillis != null) {
                timestampMillis.set(record.timestamp());
            }
            if (timestampType != null) {
                timestampType.set(record.timestampType());
            }
            if (serializedKeySize != null) {
                final int size = record.serializedKeySize();
                if (size == -1) {
                    serializedKeySize.setNull();
                } else {
                    serializedKeySize.set(size);
                }
            }
            if (serializedValueSize != null) {
                final int size = record.serializedValueSize();
                if (size == -1) {
                    serializedValueSize.setNull();
                } else {
                    serializedValueSize.set(size);
                }
            }
            if (leaderEpoch != null) {
                if (record.leaderEpoch().isPresent()) {
                    leaderEpoch.set(record.leaderEpoch().get());
                } else {
                    leaderEpoch.setNull();
                }
            }
        }
    }

    private static final class Headers {
        private static final List<Type<?>> TYPES = java.util.stream.Stream.concat(Keyk.TYPES.stream(),
                java.util.stream.Stream.of(
                        Type.stringType(),
                        Type.byteType().arrayType()))
                .collect(Collectors.toUnmodifiableList());

        public static Headers from(Stream stream) {
            return new Headers(
                    Keyk.from(stream),
                    ObjectAppender.get(stream, HEADER_KEY),
                    ObjectAppender.get(stream, HEADER_VALUE));
        }

        private final Keyk key;
        private final ObjectAppender<String> headerKey;
        private final ObjectAppender<byte[]> headerValue;

        private Headers(Keyk key, ObjectAppender<String> headerKey, ObjectAppender<byte[]> headerValue) {
            this.key = Objects.requireNonNull(key);
            this.headerKey = Objects.requireNonNull(headerKey);
            this.headerValue = Objects.requireNonNull(headerValue);
        }

        public void write(ConsumerRecord<?, ?> record, Header header) {
            key.set(record);
            headerKey.set(header.key());
            headerValue.set(header.value());
        }
    }

    private static final class Keyz<K> implements Type.Visitor<EventProcessorFactory<K>> {
        public static <T> EventProcessorFactory<T> of(Type<T> keyType) {
            return null;
        }

        public static EventProcessorSpec spec(Type<?> keyType) {
            return EventProcessorSpec.builder()
                    .usesCoordinator(false)
                    .addStreams(EventProcessorStreamSpec.builder()
                            .isRowOriented(true)
                            .expectedSize(1)
                            .addOutputTypes(keyType)
                            .build())
                    .build();
        }

        @Override
        public EventProcessorFactory<K> visit(PrimitiveType<?> primitiveType) {
            return null;
        }

        @Override
        public EventProcessorFactory<K> visit(GenericType<?> genericType) {
            // noinspection unchecked
            final GenericType<K> t = (GenericType<K>) genericType;
            return EventProcessorFactories.of(spec(genericType), sink -> {
                final Stream stream = sink.singleStream();
                final ObjectAppender<K> appender = ObjectAppender.get(stream.appenders().get(0), t);
                return EventProcessors.singleton(appender);
            });
        }
    }

}
