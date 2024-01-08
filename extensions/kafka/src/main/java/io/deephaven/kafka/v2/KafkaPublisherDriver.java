/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.kafka.KafkaTools.ConsumerLoopCallback;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;

import static io.deephaven.kafka.v2.ClientHelper.safeCloseClient;

final class KafkaPublisherDriver<K, V> implements StreamPublisher {

    public static <K, V> KafkaPublisherDriver<K, V> of(
            ClientOptions<K, V> clientOptions,
            Offsets offsets,
            KafkaStreamConsumerAdapter<K, V> streamConsumerAdapter,
            ThreadFactory threadFactory,
            ConsumerLoopCallback callback) {
        final KafkaConsumer<K, V> client =
                clientOptions.createClient(Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));
        try {
            ClientHelper.assignAndSeek(client, offsets);
            return new KafkaPublisherDriver<>(client, streamConsumerAdapter, threadFactory, callback);
        } catch (Throwable t) {
            safeCloseClient(client, t);
            throw t;
        }
    }

    private final KafkaConsumer<K, V> client;
    private final KafkaStreamConsumerAdapter<K, V> streamConsumerAdapter;
    private final ThreadFactory threadFactory;
    private final ConsumerLoopCallback callback;

    KafkaPublisherDriver(
            KafkaConsumer<K, V> client,
            KafkaStreamConsumerAdapter<K, V> streamConsumerAdapter,
            ThreadFactory threadFactory,
            ConsumerLoopCallback callback) {
        this.client = Objects.requireNonNull(client);
        this.streamConsumerAdapter = Objects.requireNonNull(streamConsumerAdapter);
        this.threadFactory = Objects.requireNonNull(threadFactory);
        this.callback = callback;
    }

    void start() {
        if (!streamConsumerAdapter.hasStreamConsumer()) {
            throw new IllegalStateException(
                    "Expected io.deephaven.kafka.v2.KafkaPublisherDriver.register to be called before start");
        }
        final Thread thread = threadFactory.newThread(this::run);
        thread.setDaemon(true);
        thread.start();
    }

    void errorBeforeStart(Throwable t) {
        if (streamConsumerAdapter.hasStreamConsumer()) {
            safeNotifyFailure(t);
        }
        streamConsumerAdapter.close();
        safeCloseClient(client, t);
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

    private void run() {
        try {
            // noinspection StatementWithEmptyBody
            while (runOnce()) {
            }
        } catch (Throwable t) {
            streamConsumerAdapter.acceptFailure(t);
        } finally {
            streamConsumerAdapter.close();
            client.close();
        }
    }

    private boolean runOnce() {
        boolean more = false;
        beforePoll();
        try {
            return (more = pollAndAccept());
        } finally {
            afterPoll(more);
        }
    }

    private void beforePoll() {
        if (callback != null) {
            callback.beforePoll(client);
        }
    }

    private void afterPoll(boolean more) {
        if (callback != null) {
            callback.afterPoll(client, more);
        }
    }

    private boolean pollAndAccept() {
        final ConsumerRecords<K, V> records;
        try {
            records = client.poll(Duration.ofMinutes(1));
        } catch (WakeupException e) {
            streamConsumerAdapter.acceptFailure(e);
            return false;
        }
        if (records.isEmpty()) {
            return true;
        }
        streamConsumerAdapter.accept(records);
        return true;
    }

    private void safeNotifyFailure(Throwable t) {
        try {
            streamConsumerAdapter.acceptFailure(t);
        } catch (Throwable t2) {
            t.addSuppressed(t2);
        }
    }
}
