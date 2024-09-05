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

        private final Sink sink;

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

            stream.advanceAll();
        }
    }

}
