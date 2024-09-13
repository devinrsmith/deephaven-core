//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.processor.factory.EventProcessorFactory.EventProcessor;
import io.deephaven.processor.factory.EventProcessors;
import io.deephaven.processor.sink.Key;
import io.deephaven.processor.sink.Keys;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

final class KafkaFactory {

    // -------------

    /**
     * The {@link ConsumerRecord#topic() topic} key.
     */
    public static final Key<String> TOPIC = Key.of("Topic", Type.stringType());

    /**
     * The {@link ConsumerRecord#partition() partition} key.
     */
    public static final Key<Integer> PARTITION = Key.of("Partition", Type.intType());

    /**
     * The {@link ConsumerRecord#offset() offset} key.
     */
    public static final Key<Long> OFFSET = Key.of("Offset", Type.longType());

    // -------------

    /**
     * The {@link ConsumerRecord#timestamp() timestamp} key.
     */
    public static final Key<Instant> TIMESTAMP = Key.of("Timestamp", Type.instantType());

    /**
     * The {@link ConsumerRecord#timestampType() timestamp type} key.
     */
    public static final Key<TimestampType> TIMESTAMP_TYPE = Key.of("TimestampType", Type.ofCustom(TimestampType.class));

    /**
     * The {@link ConsumerRecord#serializedKeySize() serialized key size} key.
     */
    public static final Key<Integer> SERIALIZED_KEY_SIZE = Key.of("SerializedKeySize", Type.intType());

    /**
     * The {@link ConsumerRecord#serializedValueSize() serialized value size} key.
     */
    public static final Key<Integer> SERIALIZED_VALUE_SIZE = Key.of("SerializedValueSize", Type.intType());

    /**
     * The {@link ConsumerRecord#leaderEpoch() leader epoch} key.
     */
    public static final Key<Integer> LEADER_EPOCH = Key.of("LeaderEpoch", Type.intType());

    /**
     * The number of {@link ConsumerRecord#headers() headers} key.
     */
    public static final Key<Integer> NUM_HEADERS = Key.of("NumHeaders", Type.intType());

    // -------------

    /**
     * The index of the header within {@link ConsumerRecord#headers() headers}.
     */
    public static final Key<Integer> HEADER_INDEX = Key.of("HeaderIndex", Type.intType());

    /**
     * The {@link Header#key() header key} key.
     */
    public static final Key<String> HEADER_KEY = Key.of("HeaderKey", Type.stringType());

    /**
     * The {@link Header#value() header value} key.
     */
    public static final Key<byte[]> HEADER_VALUE = Key.of("HeaderValue", Type.byteType().arrayType());

    // -------------

    public static void processRecord(Stream stream, RecordConsumer consumer, ConsumerRecord<?, ?> record) {
        consumer.accept(record);
        stream.advanceAll();
    }

    private static void processHeaders(Stream stream, HeaderConsumer consumer, ConsumerRecord<?, ?> record) {
        int ix = 0;
        for (Header header : record.headers()) {
            consumer.accept(record, header, ix);
            stream.advanceAll();
            ++ix;
        }
    }

    public static void t2() {

        // todo: key, value
        // todo: flatten out specific keys w/ custom parsers
        final Keys records = Keys.builder()
                .addKeys(TOPIC, PARTITION, OFFSET, TIMESTAMP, TIMESTAMP_TYPE, SERIALIZED_KEY_SIZE,
                        SERIALIZED_VALUE_SIZE, LEADER_EPOCH)
                .build();

        // todo: custom parsing
        final Keys headers = Keys.builder()
                .addKeys(TOPIC, PARTITION, OFFSET, HEADER_INDEX, HEADER_KEY, HEADER_VALUE)
                .build();


    }

    public static void what(Stream records, Stream headers) {

        final RecordConsumer recordConsumer = key(records).andThen(basics(records));
        final HeaderConsumer headerConsumer = HeaderConsumer.wrap(key(headers)).andThen(header(headers));


    }

    public static EventProcessor<ConsumerRecord<?, ?>> records(Stream stream, RecordConsumer consumer) {
        return EventProcessors.noClose(record -> processRecord(stream, consumer, record));
    }

    public static EventProcessor<ConsumerRecord<?, ?>> headers(Stream stream, HeaderConsumer consumer) {
        return EventProcessors.noClose(record -> processHeaders(stream, consumer, record));
    }


    public static RecordConsumer key(Stream stream) {
        return Keyk.from(stream);
    }

    public static RecordConsumer basics(Stream stream) {
        return Basics.from(stream);
    }

    public static RecordConsumer specificHeader(String key, Consumer<Header> consumer) {
        return record -> consumer.accept(record.headers().lastHeader(key));
    }

    public static HeaderConsumer header(Stream stream) {
        return Headers.from(stream);
    }

    // public static EventProcessorStreamSpec basics(Key<?>... keys) {
    // return EventProcessorStreamSpec.builder()
    // .addKeys(keys)
    // .isRowOriented(true)
    // .expectedSize(1)
    // .build();
    // }

    // public static EventProcessorSpec spec(Key<?>... keys) {
    // return null;
    // }

    // public static EventProcessorFactory<ConsumerRecord<?, ?>> factory(Key<?>... keys) {
    // return EventProcessorFactories.of(spec(keys), new Function<Sink, EventProcessor<ConsumerRecord<?, ?>>>() {
    // @Override
    // public EventProcessor<ConsumerRecord<?, ?>> apply(Sink sink) {
    // return null;
    // }
    // });
    // }


    // public static EventProcessorFactory<ConsumerRecord<?, ?>> basics() {
    // // todo: give the ability to append onto a singleStream(), must specify it is exactly 1 output
    // return BasicsFactory.BASICS;
    // }
    //
    // public static EventProcessorFactory<ConsumerRecord<?, ?>> headers() {
    // return HeadersFactory.HEADERS;
    // }

    // public static <K> EventProcessorFactory<ConsumerRecord<K, ?>> key(GenericType<K> keyType) {
    // final EventProcessorSpec spec = EventProcessorSpec.builder()
    // .usesCoordinator(false)
    // .addStreams(singletonSpec(keyType))
    // .build();
    // return EventProcessorFactories.map(ConsumerRecord::key, EventProcessorFactories.singleton(keyType, spec));
    // }

    // public static <V> EventProcessorFactory<ConsumerRecord<?, V>> value(GenericType<V> valueType) {
    // final EventProcessorSpec spec = EventProcessorSpec.builder()
    // .usesCoordinator(false)
    // .addStreams(singletonSpec(valueType))
    // .build();
    // return EventProcessorFactories.map(ConsumerRecord::value, EventProcessorFactories.singleton(valueType, spec));
    // }

    // private static <T> EventProcessorStreamSpec singletonSpec(GenericType<T> keyType) {
    // return EventProcessorStreamSpec.builder()
    // .isRowOriented(false)
    // .expectedSize(1)
    // .key(keyType)
    // .build();
    // }


    //
    // public static EventProcessorFactory<ConsumerRecord<?, ?>> all() {
    // return EventProcessorFactories.concat(basics(), headers(), false);
    // }



    // private enum BasicsFactory implements EventProcessorFactory<ConsumerRecord<?, ?>> {
    // BASICS;
    //
    // @Override
    // public EventProcessorSpec spec() {
    // return EventProcessorSpec.builder()
    // .usesCoordinator(false)
    // .addStreams(EventProcessorStreamSpec.builder()
    // .isRowOriented(true)
    // .expectedSize(1)
    // .addAllOutputTypes(Basics.TYPES)
    // .build())
    // .build();
    // }
    //
    // @Override
    // public EventProcessor<ConsumerRecord<?, ?>> create(Sink sink) {
    // final Basics basics = new Basics(sink.singleStream(), null);
    // return EventProcessors.noClose(basics::set);
    // }
    // }
    //
    // private enum HeadersFactory implements EventProcessorFactory<ConsumerRecord<?, ?>> {
    // HEADERS;
    //
    // @Override
    // public EventProcessorSpec spec() {
    // return EventProcessorSpec.builder()
    // .usesCoordinator(false)
    // .addStreams(EventProcessorStreamSpec.builder()
    // .isRowOriented(true)
    // .addAllOutputTypes(Headers.TYPES)
    // .build())
    // .build();
    // }
    //
    // @Override
    // public EventProcessor<ConsumerRecord<?, ?>> create(Sink sink) {
    // final Headers headers = new Headers(sink.singleStream());
    // return EventProcessors.noClose(headers::write);
    // }
    // }

    /**
     * A setter that can handle {@link #TOPIC}, {@link #PARTITION}, {@link #OFFSET}. {@link #OFFSET} is required.
     */
    private static final class Keyk implements RecordConsumer {
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

        @Override
        public void accept(ConsumerRecord<?, ?> record) {
            if (topic != null) {
                topic.set(record.topic());
            }
            if (partition != null) {
                partition.set(record.partition());
            }
            offset.set(record.offset());
        }
    }

    /**
     * A setter that can handle {@link #TIMESTAMP_TYPE}, {@link #SERIALIZED_KEY_SIZE}, {@link #SERIALIZED_VALUE_SIZE},
     * {@link #LEADER_EPOCH}, and {@link #NUM_HEADERS}.
     */
    public static final class Basics implements RecordConsumer {

        public static Basics from(Stream stream) {
            return new Basics(
                    InstantAppender.getIfPresent(stream, TIMESTAMP),
                    ObjectAppender.getIfPresent(stream, TIMESTAMP_TYPE),
                    IntAppender.getIfPresent(stream, SERIALIZED_KEY_SIZE),
                    IntAppender.getIfPresent(stream, SERIALIZED_VALUE_SIZE),
                    IntAppender.getIfPresent(stream, LEADER_EPOCH),
                    IntAppender.getIfPresent(stream, NUM_HEADERS));
        }

        private final LongAppender timestampMillis;
        private final ObjectAppender<TimestampType> timestampType;
        private final IntAppender serializedKeySize;
        private final IntAppender serializedValueSize;
        private final IntAppender leaderEpoch;
        private final IntAppender numHeaders;

        private Basics(InstantAppender timestamp, ObjectAppender<TimestampType> timestampType,
                IntAppender serializedKeySize, IntAppender serializedValueSize, IntAppender leaderEpoch,
                IntAppender numHeaders) {
            this.timestampMillis = timestamp == null ? null : timestamp.asLongEpochAppender(TimeUnit.MILLISECONDS);
            this.timestampType = timestampType;
            this.serializedKeySize = serializedKeySize;
            this.serializedValueSize = serializedValueSize;
            this.leaderEpoch = leaderEpoch;
            this.numHeaders = numHeaders;
        }

        @Override
        public void accept(ConsumerRecord<?, ?> record) {
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
            if (numHeaders != null) {
                int count = 0;
                final Iterator<Header> it = record.headers().iterator();
                while (it.hasNext()) {
                    ++count;
                    it.next();
                }
                numHeaders.set(count);
            }
        }
    }

    /**
     * A setter that can handle {@link #HEADER_INDEX}, {@link #HEADER_KEY}, and {@link #HEADER_VALUE}.
     * {@link #HEADER_KEY} and {@link #HEADER_VALUE} are required.
     */
    private static final class Headers implements HeaderConsumer {
        private static final List<Type<?>> TYPES = java.util.stream.Stream.concat(Keyk.TYPES.stream(),
                java.util.stream.Stream.of(
                        Type.stringType(),
                        Type.byteType().arrayType()))
                .collect(Collectors.toUnmodifiableList());

        public static Headers from(Stream stream) {
            return new Headers(
                    IntAppender.getIfPresent(stream, HEADER_INDEX),
                    ObjectAppender.get(stream, HEADER_KEY),
                    ObjectAppender.get(stream, HEADER_VALUE));
        }

        private final IntAppender headerIndex;
        private final ObjectAppender<String> headerKey;
        private final ObjectAppender<byte[]> headerValue;

        private Headers(IntAppender headerIndex, ObjectAppender<String> headerKey, ObjectAppender<byte[]> headerValue) {
            this.headerIndex = headerIndex;
            this.headerKey = Objects.requireNonNull(headerKey);
            this.headerValue = Objects.requireNonNull(headerValue);
        }

        @Override
        public void accept(ConsumerRecord<?, ?> record, Header header, int headerIndex) {
            if (this.headerIndex != null) {
                this.headerIndex.set(headerIndex);
            }
            headerKey.set(header.key());
            headerValue.set(header.value());
        }
    }

    // private static final class Keyz<K> implements Type.Visitor<EventProcessorFactory<K>> {
    // public static <T> EventProcessorFactory<T> of(Type<T> keyType) {
    // return null;
    // }
    //
    // public static EventProcessorSpec spec(Type<?> keyType) {
    // return EventProcessorSpec.builder()
    // .usesCoordinator(false)
    // .addStreams(EventProcessorStreamSpec.builder()
    // .isRowOriented(true)
    // .expectedSize(1)
    // .addOutputTypes(keyType)
    // .build())
    // .build();
    // }
    //
    // @Override
    // public EventProcessorFactory<K> visit(PrimitiveType<?> primitiveType) {
    // return null;
    // }
    //
    // @Override
    // public EventProcessorFactory<K> visit(GenericType<?> genericType) {
    // // noinspection unchecked
    // final GenericType<K> t = (GenericType<K>) genericType;
    // return EventProcessorFactories.of(spec(genericType), sink -> {
    // final Stream stream = sink.singleStream();
    // final ObjectAppender<K> appender = ObjectAppender.get(stream.appenders().get(0), t);
    // return EventProcessors.singleton(appender);
    // });
    // }
    // }

}
