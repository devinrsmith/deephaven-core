//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

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
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

final class KafkaFactory {

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
                .addOutputTypes(keyType)
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

        private final ObjectAppender<String> topic;
        private final IntAppender partition;
        private final LongAppender offset;

        Key(Stream stream) {
            topic = ObjectAppender.get(stream.appenders().get(0), Type.stringType());
            partition = IntAppender.get(stream.appenders().get(1));
            offset = LongAppender.get(stream.appenders().get(2));
        }

        public void set(ConsumerRecord<?, ?> record) {
            topic.set(record.topic());
            partition.set(record.partition());
            offset.set(record.offset());
        }
    }

    private static final class Basics {
        private static final List<Type<?>> TYPES = java.util.stream.Stream.concat(Key.TYPES.stream(),
                java.util.stream.Stream.of(
                        Type.instantType(),
                        Type.ofCustom(TimestampType.class),
                        Type.intType(),
                        Type.intType(),
                        Type.intType()))
                .collect(Collectors.toUnmodifiableList());

        private final Stream stream;
        private final Key key;
        private final LongAppender timestampMillis;
        private final ObjectAppender<TimestampType> timestampType;
        private final IntAppender serializedKeySize;
        private final IntAppender serializedValueSize;
        private final IntAppender leaderEpoch;

        public Basics(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            key = new Key(stream);
            timestampMillis = InstantAppender.get(stream.appenders().get(3)).asLongEpochAppender(TimeUnit.MILLISECONDS);
            timestampType = ObjectAppender.get(stream.appenders().get(4), Type.ofCustom(TimestampType.class));
            leaderEpoch = IntAppender.get(stream.appenders().get(5));
            serializedKeySize = IntAppender.get(stream.appenders().get(6));
            serializedValueSize = IntAppender.get(stream.appenders().get(7));
        }

        public void write(ConsumerRecord<?, ?> record) {
            key.set(record);
            timestampMillis.set(record.timestamp());
            timestampType.set(record.timestampType());
            if (record.leaderEpoch().isPresent()) {
                leaderEpoch.set(record.leaderEpoch().get());
            } else {
                leaderEpoch.setNull();
            }
            serializedKeySize.set(record.serializedKeySize());
            serializedValueSize.set(record.serializedValueSize());
            stream.advanceAll();
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
            key = new Key(stream);
            headerKey = ObjectAppender.get(stream.appenders().get(3), Type.stringType());
            headerValue = ObjectAppender.get(stream.appenders().get(4), Type.byteType().arrayType());
        }

        public void write(ConsumerRecord<?, ?> record) {
            for (Header header : record.headers()) {
                key.set(record);
                headerKey.set(header.key());
                headerValue.set(header.value());
                stream.advanceAll();
            }
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
