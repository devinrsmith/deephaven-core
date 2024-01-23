/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.base.clock.Clock;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadFactory;

import static io.deephaven.kafka.v2.ClientHelper.safeCloseClient;

final class PublishersImpl<K, V> implements Publishers {

    private final KafkaConsumer<K, V> client;
    private final Set<PublisherImpl<K, V>> publishers;
    private final ThreadFactory threadFactory;
    // private final ConsumerLoopCallback callback;
    private final Map<TopicPartition, PublisherImpl<K, V>> topicPartitionToPublisher;
    private final boolean receiveTimestamp;

    PublishersImpl(
            KafkaConsumer<K, V> client,
            Set<PublisherImpl<K, V>> publishers,
            ThreadFactory threadFactory,
            // ConsumerLoopCallback callback,
            boolean receiveTimestamp) {
        this.client = Objects.requireNonNull(client);
        this.publishers = Set.copyOf(publishers);
        this.threadFactory = Objects.requireNonNull(threadFactory);
        // this.callback = callback;
        this.topicPartitionToPublisher = Collections.unmodifiableMap(map(publishers));
        this.receiveTimestamp = receiveTimestamp;
    }

    private static <K, V> Map<TopicPartition, PublisherImpl<K, V>> map(Set<PublisherImpl<K, V>> publishers) {
        final Map<TopicPartition, PublisherImpl<K, V>> topicPartitionToPublisher = new HashMap<>();
        for (PublisherImpl<K, V> publisher : publishers) {
            for (TopicPartition topicPartition : publisher.topicPartitions()) {
                topicPartitionToPublisher.put(topicPartition, publisher);
            }
        }
        return topicPartitionToPublisher;
    }

    public KafkaConsumer<K, V> client() {
        return client;
    }

    @Override
    public Collection<? extends Publisher> publishers() {
        return publishers;
    }

    @Override
    public synchronized void start() {
        for (PublisherImpl<K, V> publisher : publishers) {
            if (!publisher.hasStreamConsumer()) {
                throw new IllegalStateException(
                        "Expected io.deephaven.kafka.v2.PublisherImpl.register to be called before start");
            }
        }
        final Thread thread = threadFactory.newThread(this::run);
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public synchronized void errorBeforeStart(Throwable t) {
        for (PublisherImpl<K, V> publisher : publishers) {
            if (publisher.hasStreamConsumer()) {
                safeNotifyFailure(t);
            }
        }
        safeCloseClient(client, t);
    }

    @Override
    public synchronized void close() {


        // todo: extra safety check that start or errorBeforeStart was called
    }

    private void run() {
        try {
            // noinspection StatementWithEmptyBody
            while (runOnce()) {
            }
        } catch (Throwable t) {
            safeNotifyFailure(t);
        } finally {
            for (PublisherImpl<K, V> publisher : publishers) {
                publisher.close();
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
        // if (callback != null) {
        // callback.beforePoll(client);
        // }
    }

    private void afterPoll(boolean more) {
        // if (callback != null) {
        // callback.afterPoll(client, more);
        // }
    }

    private boolean pollAndAccept() {
        final ConsumerRecords<K, V> records;
        try {
            records = client.poll(Duration.ofMinutes(1));
        } catch (WakeupException e) {
            safeNotifyFailure(e);
            return false;
        }
        final long receiveTimeEpochNanos = receiveTimestamp ? Clock.system().currentTimeNanos() : 0;
        if (records.isEmpty()) {
            return true;
        }
        accept(receiveTimeEpochNanos, records);
        return true;
    }

    private void accept(long receiveTimeEpochNanos, ConsumerRecords<K, V> records) {
        for (final TopicPartition topicPartition : records.partitions()) {
            final PublisherImpl<K, V> publisher = topicPartitionToPublisher.get(topicPartition);
            if (publisher == null) {
                throw new IllegalStateException(String.format("Received unexpected topicPartition=%s", topicPartition));
            }
            publisher.fillImpl(receiveTimeEpochNanos, topicPartition, records.records(topicPartition));
        }
    }

    private void safeNotifyFailure(Throwable t) {
        for (PublisherImpl<K, V> consumer : publishers) {
            try {
                consumer.acceptFailure(t);
            } catch (Throwable t2) {
                t.addSuppressed(t2);
            }
        }
    }
}
