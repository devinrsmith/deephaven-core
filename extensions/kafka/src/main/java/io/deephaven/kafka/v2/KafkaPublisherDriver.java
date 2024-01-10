/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.kafka.KafkaTools.ConsumerLoopCallback;
import io.deephaven.kafka.v2.KafkaOptions.Partitioning;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

import static io.deephaven.kafka.v2.ClientHelper.safeCloseClient;

final class KafkaPublisherDriver<K, V> {

    public static <K, V> KafkaPublisherDriver<K, V> of(
            ClientOptions<K, V> clientOptions,
            Offsets offsets,
            Partitioning partitioning,
            ThreadFactory threadFactory,
            ConsumerLoopCallback callback) {
        final KafkaConsumer<K, V> client =
                clientOptions.createClient(Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));
        try {
            final Map<TopicPartition, Offset> map = ClientHelper.assignAndSeek(client, offsets);
            final Runnable onShutdown = client::wakeup;

            ((PartitioningBase) partitioning

            new KafkaPublisher<>(null, null, null, onShutdown, 1024, true);


            final Set<KafkaPublisher<K, V>> kk = null;
            return new KafkaPublisherDriver<>(client, kk, threadFactory, callback);
        } catch (Throwable t) {
            safeCloseClient(client, t);
            throw t;
        }
    }

    private final KafkaConsumer<K, V> client;
    private final Set<KafkaPublisher<K, V>> publishers;
    private final Map<TopicPartition, KafkaPublisher<K, V>> topicPartitionToConsumer;
    private final ThreadFactory threadFactory;
    private final ConsumerLoopCallback callback;

    KafkaPublisherDriver(
            KafkaConsumer<K, V> client,
            Set<KafkaPublisher<K, V>> publishers,
            ThreadFactory threadFactory,
            ConsumerLoopCallback callback) {
        this.client = Objects.requireNonNull(client);
        this.publishers = Set.copyOf(publishers);
        topicPartitionToConsumer = null; // todo
        this.threadFactory = Objects.requireNonNull(threadFactory);
        this.callback = callback;
    }

    void start() {
        for (KafkaPublisher<K, V> publisher : publishers) {
            if (!publisher.hasStreamConsumer()) {
                throw new IllegalStateException(
                        "Expected io.deephaven.kafka.v2.KafkaPublisherDriver.register to be called before start");
            }
        }
        final Thread thread = threadFactory.newThread(this::run);
        thread.setDaemon(true);
        thread.start();
    }

    void errorBeforeStart(Throwable t) {
        for (KafkaPublisher<K, V> publisher : publishers) {
            if (publisher.hasStreamConsumer()) {
                safeNotifyFailure(t);
            }
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
            safeNotifyFailure(t);
        } finally {
            for (KafkaPublisher<K, V> consumer : what) {
                consumer.close();
            }
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
            safeNotifyFailure(e);
            return false;
        }
        if (records.isEmpty()) {
            return true;
        }
        accept(records);
        return true;
    }

    private void accept(ConsumerRecords<K, V> records) {
        for (final TopicPartition topicPartition : records.partitions()) {
            final KafkaPublisher<K, V> consumer = topicPartitionToConsumer.get(topicPartition);
            consumer.fillImpl(0, topicPartition, records.records(topicPartition));
        }
    }

    private void safeNotifyFailure(Throwable t) {
        for (KafkaPublisher<K, V> consumer : publishers) {
            try {
                consumer.acceptFailure(t);
            } catch (Throwable t2) {
                t.addSuppressed(t2);
            }
        }
    }
}
