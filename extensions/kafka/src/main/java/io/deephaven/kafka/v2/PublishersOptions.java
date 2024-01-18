/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.kafka.KafkaTools.ConsumerLoopCallback;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.util.thread.NamingThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.immutables.value.Value.Immutable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.kafka.v2.ClientHelper.safeCloseClient;

@Immutable
@BuildableStyle
public abstract class PublishersOptions<K, V> {

    public static <K, V> Builder<K, V> builder() {
        return ImmutablePublishersOptions.builder();
    }

    /**
     * The Kafka client options.
     *
     * @return the Kafka client options
     */
    public abstract ClientOptions<K, V> clientOptions();

    /**
     * The partitioning strategy.
     *
     * @return the partitioning strategy
     */
    public abstract Partitioning partitioning();

    /**
     * The offsets.
     *
     * @return the offsets
     */
    public abstract Offsets offsets();

    /**
     * The {@link ConsumerRecord} filter. The filtering happens before {@link #processor()}.
     *
     * @return the consumer record filter
     */
    public abstract Predicate<ConsumerRecord<K, V>> filter();

    /**
     * The {@link ConsumerRecord} processor.
     *
     * @return the consumer record processor
     */
    public abstract ObjectProcessor<ConsumerRecord<K, V>> processor();

    /**
     * The maximum size of each chunk that will be passed to a {@link StreamConsumer}.
     *
     * @return the chunk size
     */
    public abstract int chunkSize();

    public abstract boolean receiveTimestamp();

    public abstract Optional<ConsumerLoopCallback> callback();

    public interface Builder<K, V> {
        Builder<K, V> clientOptions(ClientOptions<K, V> clientOptions);

        Builder<K, V> partitioning(Partitioning partitioning);

        Builder<K, V> offsets(Offsets offsets);

        Builder<K, V> filter(Predicate<ConsumerRecord<K, V>> filter);

        Builder<K, V> processor(ObjectProcessor<ConsumerRecord<K, V>> processor);

        Builder<K, V> chunkSize(int chunkSize);

        Builder<K, V> receiveTimestamp(boolean receiveTimestamp);

        PublishersOptions<K, V> build();
    }

    public interface Partitioning {

        /**
         * The single group partitioning.
         *
         * @return the partitioning
         */
        static Partitioning single() {
            return SinglePartitioning.INSTANCE;
        }

        /**
         * Partitioning via {@link TopicPartition#topic()} and {@link TopicPartition#partition()}.
         *
         * @return the partitioning
         */
        static Partitioning perTopicPartition() {
            return PerTopicPartitionPartitioning.INSTANCE;
        }

        /**
         * Partitioning via {@link TopicPartition#topic()}.
         *
         * @return the partitioning
         */
        static Partitioning perTopic() {
            return PerTopicPartitioning.INSTANCE;
        }

        /**
         * Partitioning via {@link TopicPartition#partition()}.
         *
         * @return the partitioning
         */
        static Partitioning perPartition() {
            return PerPartitionPartitioning.INSTANCE;
        }
    }

    final Publishers publishers() {
        return publishersImpl();
    }

    private PublishersImpl<K, V> publishersImpl() {
        // todo: error if clientOptions contains enable auto commit
        // final KafkaConsumer<K, V> client =
        // clientOptions().createClient(Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));
        final KafkaConsumer<K, V> client =
                clientOptions().createClient(null);
        try {
            final Set<TopicPartition> topicPartitions = ClientHelper.assignAndSeek(client, offsets());
            // any one calling shutdown
            final Runnable onShutdown = client::wakeup;
            final Set<PublisherImpl<K, V>> publishers = ((PartitioningBase) partitioning())
                    .partition(topicPartitions)
                    .map(tp -> new PublisherImpl<>(tp, filter(), processor(), onShutdown, chunkSize(),
                            receiveTimestamp()))
                    .collect(Collectors.toSet());
            return new PublishersImpl<>(client, publishers, threadFactory(), callback().orElse(null),
                    receiveTimestamp());
        } catch (Throwable t) {
            safeCloseClient(client, t);
            throw t;
        }
    }

    // Maybe expose as configuration option in future?
    private static ThreadFactory threadFactory() {
        return new NamingThreadFactory(null, TableOptions.class, "KafkaPublisherDriver", true);
    }

    abstract static class PartitioningBase implements Partitioning {

        // If we think there is value in allowing users to manually partition, we can instead expose this as the
        // interface for Partitioning directly.
        abstract Stream<Set<TopicPartition>> partition(Set<TopicPartition> topicPartitions);
    }

    private static class SinglePartitioning extends PartitioningBase {
        private static final SinglePartitioning INSTANCE = new SinglePartitioning();

        @Override
        Stream<Set<TopicPartition>> partition(Set<TopicPartition> topicPartitions) {
            return Stream.of(topicPartitions);
        }
    }

    private static class PerTopicPartitionPartitioning extends PartitioningBase {
        private static final PerTopicPartitionPartitioning INSTANCE = new PerTopicPartitionPartitioning();

        @Override
        Stream<Set<TopicPartition>> partition(Set<TopicPartition> topicPartitions) {
            return topicPartitions.stream().map(Collections::singleton);
        }
    }

    private static class PerTopicPartitioning extends PartitioningBase {
        private static final PerTopicPartitioning INSTANCE = new PerTopicPartitioning();

        @Override
        Stream<Set<TopicPartition>> partition(Set<TopicPartition> topicPartitions) {
            final Map<String, Set<TopicPartition>> map = new HashMap<>();
            for (TopicPartition topicPartition : topicPartitions) {
                map.computeIfAbsent(topicPartition.topic(), ignored -> new HashSet<>()).add(topicPartition);
            }
            return map.values().stream();
        }
    }

    private static class PerPartitionPartitioning extends PartitioningBase {
        private static final PerPartitionPartitioning INSTANCE = new PerPartitionPartitioning();

        @Override
        Stream<Set<TopicPartition>> partition(Set<TopicPartition> topicPartitions) {
            final Map<Integer, Set<TopicPartition>> map = new HashMap<>();
            for (TopicPartition topicPartition : topicPartitions) {
                map.computeIfAbsent(topicPartition.partition(), ignored -> new HashSet<>()).add(topicPartition);
            }
            return map.values().stream();
        }
    }
}
