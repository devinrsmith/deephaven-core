/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.v2;

import io.deephaven.base.clock.Clock;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * A collection of {@link }
 */
public interface Offsets {

    /**
     * The topic-partition offset.
     *
     * @param topicPartition the topic partition
     * @param offset the offset
     * @return the topic-partition offset
     */
    static Offsets of(TopicPartition topicPartition, long offset) {
        return ImmutableOffsetsExplicitPartition.of(topicPartition, offset);
    }

    /**
     * The combined {@code offsets}.
     *
     * @param offsets the offsets
     * @return the offsets
     */
    static Offsets of(Offsets... offsets) {
        return of(Arrays.asList(offsets));
    }

    /**
     * The combined {@code offsets}.
     *
     * @param offsets the offsets
     * @return the offsets
     */
    static Offsets of(List<Offsets> offsets) {
        return ImmutableOffsetsList.of(offsets);
    }

    /**
     * The beginning offsets for all partitions of {@code topic}.
     *
     * @param topic the topic
     * @return the beginning offsets
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets(Collection)
     */
    static Offsets beginning(String topic) {
        return ImmutableOffsetsBeginning.of(topic);
    }

    /**
     * The beginning offset for {@code topicPartition}.
     *
     * @param topicPartition the topic partition
     * @return the beginning offset
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets(Collection)
     */
    static Offsets beginning(TopicPartition topicPartition) {
        return ImmutableOffsetsBeginningPartition.of(topicPartition);
    }

    /**
     * The end offsets for all partitions of {@code topic}.
     *
     * @param topic the topic
     * @return the end offsets
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets(Collection)
     */
    static Offsets end(String topic) {
        return ImmutableOffsetsEnd.of(topic);
    }

    /**
     * The end offset for {@code topicPartition}
     *
     * @param topicPartition the topic partition
     * @return the end offset
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#endOffsets(Collection)
     */
    static Offsets end(TopicPartition topicPartition) {
        return ImmutableOffsetsEndPartition.of(topicPartition);
    }

    /**
     * The committed offset for all partitions of {@code topic} (whether the commits happened by this process or
     * another).
     *
     * @param topic the topic
     * @return the committed offsets
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#committed(Set)
     */
    static Offsets committed(String topic) {
        return ImmutableOffsetsCommitted.of(topic);
    }

    /**
     * The committed offset for {@code topicPartition} (whether the commits happened by this process or another).
     *
     * @param topicPartition the topic partition
     * @return the committed offset
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#committed(Set)
     */
    static Offsets committed(TopicPartition topicPartition) {
        return ImmutableOffsetsCommittedPartition.of(topicPartition);
    }

    /**
     * The earliest offset whose timestamp is greater than or equal to {@code since} for all partitions of
     * {@code topic}.
     * 
     * @param topic the topic
     * @param since the timestamp
     * @return the timestamp offsets
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes(Map)
     */
    static Offsets timestamp(String topic, Instant since) {
        return ImmutableOffsetsTimestamp.of(topic, since);
    }

    /**
     * The earliest offset whose timestamp is greater than or equal to {@code since} for {@code topicPartition}.
     *
     * @param topicPartition the topic partition
     * @param since the timestamp
     * @return the timestamp offset
     * @see org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes(Map)
     */
    static Offsets timestamp(TopicPartition topicPartition, Instant since) {
        return ImmutableOffsetsTimestampPartition.of(topicPartition, since);
    }

    /**
     * The earliest offset whose timestamp is at most {@code age} old for all partitions of {@code topic}. Equivalent to
     * {@code timestamp(topic, Clock.system().instantMillis().minus(ago))}.
     *
     * @param topic the topic
     * @param ago the age
     * @return the timestamp offsets
     * @see #timestamp(String, Instant)
     */
    static Offsets timestamp(String topic, Duration ago) {
        return timestamp(topic, Clock.system().instantMillis().minus(ago));
    }

    /**
     * The earliest offset whose timestamp is at most {@code age} old for {@code topicPartition}. Equivalent to
     * {@code timestamp(topicPartition, Clock.system().instantMillis().minus(ago))}.
     *
     * @param topicPartition the topic partition
     * @param ago the age
     * @return the timestamp offset
     * @see #timestamp(String, Instant)
     */
    static Offsets timestamp(TopicPartition topicPartition, Duration ago) {
        return timestamp(topicPartition, Clock.system().instantMillis().minus(ago));
    }

    /**
     * The topic partitions of {@code offsets} that match {@code partitionFilter}. Equivalent to
     * {@code filter(offsets, topicPartition -> partitionFilter.test(topicPartition.partition()))}.
     *
     * @param offsets the offsets
     * @param partitionFilter the partition filter
     * @return the filtered topic partitions
     * @see #filter(Offsets, Predicate)
     */
    static Offsets filterPartition(Offsets offsets, IntPredicate partitionFilter) {
        return filter(offsets, topicPartition -> partitionFilter.test(topicPartition.partition()));
    }

    /**
     * The topic partitions of {@code offsets} that match {@code topicPartitionFilter}.
     *
     * @param offsets the offsets
     * @param topicPartitionFilter the topic partition filter
     * @return the filtered topic partitions
     */
    static Offsets filter(Offsets offsets, Predicate<TopicPartition> topicPartitionFilter) {
        return ImmutableOffsetsFiltered.of(offsets, topicPartitionFilter);
    }
}
