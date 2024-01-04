/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Objects;

import static io.deephaven.kafka.v2.KafkaToolsNew.safeCloseClient;

final class KafkaPublisherDriver<K, V> implements StreamPublisher {

    public static <K, V> KafkaPublisherDriver<K, V> of(
            ClientOptions<K, V> clientOptions,
            SubscribeOptions subscribeOptions,
            KafkaStreamConsumerAdapter<K, V> pipe) {
        final KafkaConsumer<K, V> client = clientOptions.createClient();
        try {
            ClientHelper.assignAndSeek(client, subscribeOptions);
            return new KafkaPublisherDriver<>(client, pipe);
        } catch (Throwable t) {
            safeCloseClient(t, client);
            throw t;
        }
    }

    private final KafkaConsumer<K, V> client;
    private final KafkaStreamConsumerAdapter<K, V> streamConsumerAdapter;

    KafkaPublisherDriver(KafkaConsumer<K, V> client, KafkaStreamConsumerAdapter<K, V> streamConsumerAdapter) {
        this.client = Objects.requireNonNull(client);
        this.streamConsumerAdapter = Objects.requireNonNull(streamConsumerAdapter);
    }

    void start() {
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
                    streamConsumerAdapter.accept(records);
                }
            } catch (Throwable t) {
                notifyFailure(t);
                throw t;
            } finally {
                client.close();
            }
        });
        // todo name
        thread.setDaemon(true);
        thread.start();
    }

    void startError(Throwable t) {
        if (streamConsumerAdapter.hasStreamConsumer()) {
            notifyFailure(t);
        }
        safeCloseClient(t, client);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        streamConsumerAdapter.init(consumer);
    }

    @Override
    public void flush() {
        streamConsumerAdapter.flush();
    }

    @Override
    public void shutdown() {
        client.wakeup();
    }

    private void notifyFailure(Throwable t) {
        try {
            streamConsumerAdapter.acceptFailure(t);
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
