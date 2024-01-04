/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class KafkaStreamPublisherImpl<K, V> implements StreamPublisher {

    private final ObjectProcessor<ConsumerRecord<K, V>> processor;
    private KafkaPipe<K, V> pipe;

    KafkaStreamPublisherImpl(ObjectProcessor<ConsumerRecord<K, V>> processor) {
        this.processor = Objects.requireNonNull(processor);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (pipe != null) {
            throw new IllegalStateException();
        }
        pipe = new KafkaPipe<>(processor, consumer, 1024);
    }

    public void fill(ConsumerRecords<K, V> records) {
        pipe.fill(records);
    }

    public void acceptFailure(Throwable cause) {
        pipe.acceptFailure(cause);
    }

    @Override
    public void flush() {
        pipe.flush();
    }

    @Override
    public void shutdown() {
        // todo, callback for driver?
        // for example, unsubscribe to these ConsumerRecords
        // todo
    }
}
