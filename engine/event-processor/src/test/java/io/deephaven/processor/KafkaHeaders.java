//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.processor.factory.EventProcessorFactory;
import io.deephaven.processor.factory.EventProcessorSpec;
import io.deephaven.processor.sink.Sink;
import io.deephaven.processor.sink.Stream;
import io.deephaven.processor.sink.appender.InstantAppender;
import io.deephaven.processor.sink.appender.IntAppender;
import io.deephaven.processor.sink.appender.LongAppender;
import io.deephaven.processor.sink.appender.ObjectAppender;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

final class KafkaHeaders implements EventProcessorFactory<ConsumerRecord<?, ?>> {

    @Override
    public EventProcessorSpec spec() {
        return EventProcessorSpec.builder()
                .usesCoordinator(false)
                .addStreams()
                .build();
    }

    @Override
    public EventProcessor<ConsumerRecord<?, ?>> create(Sink sink) {
        return null;
    }

    private static final class What implements EventProcessor<ConsumerRecord<?, ?>> {

        // private final Sink sink;

        @Override
        public void writeToSink(ConsumerRecord<?, ?> event) {

        }

        @Override
        public void close() {

        }
    }

    private static final class Basic {
        private final Stream stream;
        private final ObjectAppender<String> topic;
        private final IntAppender partition;
        private final LongAppender offset;
        private final LongAppender timestampMillis;
        private final ObjectAppender<TimestampType> timestampType = null;
        private final IntAppender leaderEpoch = null;

        public Basic(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            topic = ObjectAppender.get(stream.appenders().get(0), Type.stringType());
            partition = IntAppender.get(stream.appenders().get(1));
            offset = LongAppender.get(stream.appenders().get(2));
            timestampMillis = InstantAppender.get(stream.appenders().get(0)).asLongEpochAppender(TimeUnit.MILLISECONDS);
        }

        public void writeToAppenders(ConsumerRecord<?, ?> record) {
            topic.set(record.topic());
            partition.set(record.partition());
            offset.set(record.offset());
            timestampMillis.set(record.timestamp());
            timestampType.set(record.timestampType());
            if (record.leaderEpoch().isPresent()) {
                leaderEpoch.set(record.leaderEpoch().get());
            } else {
                leaderEpoch.setNull();
            }
            // todo: key, value
            stream.advanceAll();
        }
    }

    private static final class Headers {
        private final Stream stream;
        // private final ObjectAppender<String> key;
        // private final ObjectAppender<byte[]> value;

        public Headers(Stream stream) {
            this.stream = Objects.requireNonNull(stream);
            // key = stream.appenders().get(0);
        }

        public void writeToAppenders(org.apache.kafka.common.header.Headers headers) {
            for (Header header : headers) {
                // key.set(header.key());
                // value.set(header.value());
                stream.advanceAll();
            }
        }
    }

    interface Heads {

        static void addAll(Heads heads, org.apache.kafka.common.header.Headers headers) {
            for (Header header : headers) {
                heads.add(header.key(), header.value());
            }
        }

        void add(String key, byte[] value);
    }

    interface H {
        static void setAll(H h, org.apache.kafka.common.header.Headers headers) {
            for (Header header : headers) {
                set(h, header);
            }
        }

        static void set(H h, Header header) {
            h.key(header.key());
            h.value(header.value());
            h.advance();
        }

        void key(String key);

        void value(byte[] value);

        void advance();
    }

}
