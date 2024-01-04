/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;

final class KafkaPublisherDriver<K, V> implements StreamPublisher {

    public static <K, V> KafkaPublisherDriver<K, V> of(
            ClientOptions<K, V> clientOptions,
            ObjectProcessor<ConsumerRecord<K, V>> processor,
            int chunkSize,
            Collection<TopicPartition> topicPartitions) {
        final KafkaConsumer<K, V> client = clientOptions.createClient();
        // todo seek
        client.assign(topicPartitions);
        client.seekToEnd(topicPartitions);
        return new KafkaPublisherDriver<>(client, new KafkaPipe2<>(processor, chunkSize));
    }

    private final KafkaConsumer<K, V> client;
    private final KafkaPipe2<K, V> pipe;

    private KafkaPublisherDriver(KafkaConsumer<K, V> client, KafkaPipe2<K, V> pipe) {
        this.client = Objects.requireNonNull(client);
        this.pipe = Objects.requireNonNull(pipe);
    }

    public void start() {
        final Thread thread = new Thread(() -> {
            try {
                while (true) {
                    final ConsumerRecords<K, V> records;
                    try {
                        records = client.poll(Duration.ofMinutes(1));
                    } catch (WakeupException e) {
                        notifyFailure(new RuntimeException("shutdown"));
                        return;
                    }
                    if (records.isEmpty()) {
                        continue;
                    }
                    pipe.fill(records);
                }
            } catch (Throwable t) {
                notifyFailure(t);
                throw t;
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        pipe.init(consumer);
    }

    @Override
    public void flush() {
        pipe.flush();
    }

    @Override
    public void shutdown() {
        client.wakeup();
    }

    private void notifyFailure(Throwable t) {
        try {
            pipe.acceptFailure(t);
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
